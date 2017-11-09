use channel::poll::{PollEvent, PollingLoop, ProcessResult, RpcPollEvent, RpcPollingLoop};
use channel::tcp::TcpSender;
use channel;
use dataflow::prelude::*;
use dataflow::{backlog, checktable, node, payload, DomainConfig, PersistenceParameters, Readers};
use dataflow::ops::base::Base;
use dataflow::statistics::GraphStats;
use worker::Worker;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::sync::{Arc, Barrier, Mutex};
use std::sync::mpsc;
use std::{fmt, io, thread, time};

use futures::{Future, Stream};
use hyper::Client;
use mio::net::TcpListener;
use petgraph;
use petgraph::visit::Bfs;
use petgraph::graph::NodeIndex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use slog;
use tarpc::sync::client::{self, ClientExt};
use tokio_core::reactor::Core;

pub mod coordination;
pub mod domain_handle;
pub mod keys;
pub mod migrate;

mod mutator;
mod getter;

use self::domain_handle::DomainHandle;
use self::coordination::{CoordinationMessage, CoordinationPayload};

pub use self::mutator::{Mutator, MutatorBuilder, MutatorError};
pub use self::getter::{Getter, ReadQuery, ReadReply, RemoteGetter, RemoteGetterBuilder};
use self::payload::{EgressForBase, IngressFromBase};


pub type WorkerIdentifier = SocketAddr;
pub type WorkerEndpoint = Arc<Mutex<TcpSender<CoordinationMessage>>>;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[derive(Clone)]
struct WorkerStatus {
    healthy: bool,
    last_heartbeat: Instant,
    sender: Arc<Mutex<TcpSender<CoordinationMessage>>>,
}

impl WorkerStatus {
    pub fn new(sender: Arc<Mutex<TcpSender<CoordinationMessage>>>) -> Self {
        WorkerStatus {
            healthy: true,
            last_heartbeat: Instant::now(),
            sender,
        }
    }
}


/// Used to construct a controller.
pub struct ControllerBuilder {
    sharding_enabled: bool,
    domain_config: DomainConfig,
    persistence: PersistenceParameters,
    materializations: migrate::materialization::Materializations,
    listen_addr: IpAddr,
    heartbeat_every: Duration,
    healthcheck_every: Duration,
    nworkers: usize,
    internal_port: u16,
    external_port: u16,
    log: slog::Logger,
}
impl Default for ControllerBuilder {
    fn default() -> Self {
        let log = slog::Logger::root(slog::Discard, o!());
        Self {
            sharding_enabled: true,
            domain_config: DomainConfig {
                concurrent_replays: 512,
                replay_batch_timeout: time::Duration::from_millis(1),
                replay_batch_size: 32,
            },
            persistence: Default::default(),
            materializations: migrate::materialization::Materializations::new(&log),
            listen_addr: "127.0.0.1".parse().unwrap(),
            heartbeat_every: Duration::from_secs(1),
            healthcheck_every: Duration::from_secs(10),
            internal_port: if cfg!(test) { 0 } else { 8000 },
            external_port: if cfg!(test) { 0 } else { 9000 },
            nworkers: 0,
            log,
        }
    }
}
impl ControllerBuilder {
    /// Set the maximum number of concurrent partial replay requests a domain can have outstanding
    /// at any given time.
    ///
    /// Note that this number *must* be greater than the width (in terms of number of ancestors) of
    /// the widest union in the graph, otherwise a deadlock will occur.
    pub fn set_max_concurrent_replay(&mut self, n: usize) {
        self.domain_config.concurrent_replays = n;
    }

    /// Set the maximum number of partial replay responses that can be aggregated into a single
    /// replay batch.
    pub fn set_partial_replay_batch_size(&mut self, n: usize) {
        self.domain_config.replay_batch_size = n;
    }

    /// Set the longest time a partial replay response can be delayed.
    pub fn set_partial_replay_batch_timeout(&mut self, t: time::Duration) {
        self.domain_config.replay_batch_timeout = t;
    }

    /// Set the persistence parameters used by the system.
    pub fn set_persistence(&mut self, p: PersistenceParameters) {
        self.persistence = p;
    }

    /// Disable partial materialization for all subsequent migrations
    pub fn disable_partial(&mut self) {
        self.materializations.disable_partial();
    }

    /// Disable sharding for all subsequent migrations
    pub fn disable_sharding(&mut self) {
        self.sharding_enabled = false;
    }

    /// Set how many workers the controller should wait for before starting. More workers can join
    /// later, but they won't be assigned any of the initial domains.
    pub fn set_nworkers(&mut self, workers: usize) {
        self.nworkers = workers;
    }

    #[cfg(test)]
    pub fn build_inner(self) -> ControllerInner {
        ControllerInner::from_builder(self)
    }

    /// Build a controller, and return a Blender to provide access to it.
    pub fn build(self) -> Blender {
        // TODO(fintelia): Don't hard code addresses in this function.
        let internal_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), self.internal_port);
        let external_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), self.external_port);
        let nworkers = self.nworkers;

        let inner = Arc::new(Mutex::new(ControllerInner::from_builder(self)));
        let addr = ControllerInner::listen_external(inner.clone(), external_addr);
        ControllerInner::listen_internal(inner, internal_addr, nworkers);

        Blender {
            url: format!("http://{}", addr),
        }
    }
}

/// `Controller` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `ControllerInner::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub struct ControllerInner {
    ingredients: petgraph::Graph<node::Node, Edge>,
    source: NodeIndex,
    ndomains: usize,
    checktable: checktable::CheckTableClient,
    checktable_addr: SocketAddr,
    sharding_enabled: bool,

    domain_config: DomainConfig,

    /// Parameters for persistence code.
    persistence: PersistenceParameters,
    materializations: migrate::materialization::Materializations,

    domains: HashMap<DomainIndex, DomainHandle>,
    channel_coordinator: Arc<ChannelCoordinator>,
    debug_channel: Option<SocketAddr>,

    listen_addr: IpAddr,
    read_listen_addr: SocketAddr,
    readers: Readers,

    /// Map from worker address to the address the worker is listening on for reads.
    read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    workers: HashMap<WorkerIdentifier, WorkerStatus>,

    /// State between migrations
    deps: HashMap<DomainIndex, (IngressFromBase, EgressForBase)>,
    remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    heartbeat_every: Duration,
    healthcheck_every: Duration,
    last_checked_workers: Instant,

    log: slog::Logger,
}

impl ControllerInner {
    /// Listen for messages from clients.
    pub fn listen_external(
        controller: Arc<Mutex<ControllerInner>>,
        addr: SocketAddr,
    ) -> SocketAddr {
        use rustful::{Context, Handler, Response, Server, TreeRouter};

        enum C<Q, R> {
            OpMut(Box<Fn(&mut ControllerInner, Q) -> R + Send + Sync>),
            Op(Box<Fn(&ControllerInner, Q) -> R + Send + Sync>),
        }
        impl<Q: DeserializeOwned + 'static, R: Serialize + 'static> Handler for C<Q, R> {
            fn handle_request(&self, ctx: Context, res: Response) {
                let controller = ctx.global.get::<Arc<Mutex<ControllerInner>>>().unwrap();
                let mut controller = controller.lock().unwrap();

                let q = serde_json::from_reader(ctx.body).unwrap();
                let r = match *self {
                    C::Op(ref f) => f(&mut controller, q),
                    C::OpMut(ref f) => f(&mut controller, q),
                };
                res.send(serde_json::to_string(&r).unwrap());
            }
        }
        impl<Q: DeserializeOwned + 'static, R: Serialize + 'static> C<Q, R> {
            fn handler(self) -> Box<Handler> {
                Box::new(self) as Box<Handler>
            }
        }

        let handlers = insert_routes!{
            TreeRouter::new() => {
                "inputs" => Post: C::Op(Box::new(Self::inputs)).handler(),
                "outputs" => Post: C::Op(Box::new(Self::outputs)).handler(),
                "mutator_builder" => Post: C::Op(Box::new(Self::get_mutator_builder)).handler(),
                "getter_builder" =>  Post: C::Op(Box::new(Self::get_getter_builder)).handler(),
                "get_statistics" => Post: C::OpMut(Box::new(Self::get_statistics)).handler(),
                "install_recipe" => Post: C::OpMut(Box::new(Self::install_recipe)).handler(),
            }
        };

        let listen = Server {
            handlers,
            host: addr.into(),
            server: "netsoup".to_owned(),
            threads: Some(1),
            content_type: "application/json".parse().unwrap(),
            global: Box::new(controller).into(),
            ..Server::default()
        }.run()
            .unwrap();

        // Bit of a dance to return socket while keeping the server running in another thread.
        let socket = listen.socket.clone();
        thread::spawn(move || drop(listen));
        socket
    }

    /// Listen for messages from workers.
    pub fn listen_internal(
        controller: Arc<Mutex<ControllerInner>>,
        addr: SocketAddr,
        nworkers: usize,
    ) {
        use channel::poll::ProcessResult;
        use mio::net::TcpListener;
        use std::str::FromStr;

        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut workers_arrived = false;
            let mut pl: PollingLoop<CoordinationMessage> = PollingLoop::new(addr);
            pl.run_polling_loop(|e| {
                let mut inner = controller.lock().unwrap();
                match e {
                    PollEvent::Process(ref msg) => {
                        trace!(inner.log, "Received {:?}", msg);
                        let process = match msg.payload {
                            CoordinationPayload::Register {
                                ref addr,
                                ref read_listen_addr,
                            } => inner.handle_register(msg, addr, read_listen_addr.clone()),
                            CoordinationPayload::Heartbeat => inner.handle_heartbeat(msg),
                            CoordinationPayload::DomainBooted(ref domain, ref addr) => Ok(()),
                            _ => unimplemented!(),
                        };
                        match process {
                            Ok(_) => (),
                            Err(e) => {
                                error!(inner.log, "failed to handle message {:?}: {:?}", msg, e)
                            }
                        }
                    }
                    PollEvent::ResumePolling(timeout) => *timeout = Some(inner.healthcheck_every),
                    PollEvent::Timeout => (),
                }

                // TODO(fintelia): Avoid checking worker liveness more frequently than
                // inner.healthcheck_every.
                inner.check_worker_liveness();

                if !workers_arrived && inner.workers.len() == nworkers {
                    workers_arrived = true;
                    tx.send(());
                }

                ProcessResult::KeepPolling
            });
        });

        // Wait for enough workers to join.
        let _ = rx.recv();
    }

    fn handle_register(
        &mut self,
        msg: &CoordinationMessage,
        remote: &SocketAddr,
        read_listen_addr: SocketAddr,
    ) -> Result<(), io::Error> {
        info!(
            self.log,
            "new worker registered from {:?}, which listens on {:?}",
            msg.source,
            remote
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

    /// Construct `Controller` with a specified listening interface
    pub fn from_builder(builder: ControllerBuilder) -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
            true,
        ));

        let addr = SocketAddr::new(builder.listen_addr.clone(), 0);
        let checktable_addr = checktable::service::CheckTableServer::start(addr.clone());
        let checktable =
            checktable::CheckTableClient::connect(checktable_addr, client::Options::default())
                .unwrap();

        let readers: Readers = Arc::default();
        let readers_clone = readers.clone();
        let read_polling_loop = RpcPollingLoop::new(addr.clone());
        let read_listen_addr = read_polling_loop.get_listener_addr().unwrap();
        thread::spawn(move || {
            Worker::serve_reads(read_polling_loop, readers_clone)
        });

        ControllerInner {
            ingredients: g,
            source: source,
            ndomains: 0,
            checktable,
            checktable_addr,

            sharding_enabled: builder.sharding_enabled,
            materializations: builder.materializations,
            domain_config: builder.domain_config,
            persistence: builder.persistence,
            listen_addr: builder.listen_addr,
            heartbeat_every: builder.heartbeat_every,
            healthcheck_every: builder.healthcheck_every,
            log: builder.log,

            domains: Default::default(),
            channel_coordinator: Arc::new(ChannelCoordinator::new()),
            debug_channel: None,

            deps: HashMap::default(),
            remap: HashMap::default(),

            readers,
            read_listen_addr,
            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            last_checked_workers: Instant::now(),
        }
    }

    /// Use a debug channel. This function may only be called once because the receiving end it
    /// returned.
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
    pub fn with_persistence_options(&mut self, params: PersistenceParameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
        self.materializations.set_logger(&self.log);
    }

    /// Adds a new user universe to the Blender.
    /// User universes automatically enforce security policies.
    pub fn add_universe(&mut self, context: HashMap<String, DataType>) -> Migration {
        info!(self.log, "Adding a new Soup universe");
        let miglog = self.log.new(o!());
        Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),

            start: time::Instant::now(),
            log: miglog,
            context: context,
        }
    }

    /// Start setting up a new `Migration`.
    #[deprecated]
    pub fn start_migration(&mut self) -> Migration {
        info!(self.log, "starting migration");
        let miglog = self.log.new(o!());
        Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context: Default::default(),

            start: time::Instant::now(),
            log: miglog,
        }
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
    pub fn get_validator(&self) -> Box<Fn(&checktable::Token) -> bool> {
        let checktable =
            checktable::CheckTableClient::connect(self.checktable_addr, client::Options::default())
                .unwrap();
        Box::new(move |t: &checktable::Token| {
            checktable.validate_token(t.clone()).unwrap()
        })
    }

    #[cfg(test)]
    pub fn graph(&self) -> &Graph {
        &self.ingredients
    }

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Base`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    pub fn inputs(&self, _: ()) -> BTreeMap<String, NodeIndex> {
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
    pub fn outputs(&self, _: ()) -> BTreeMap<String, NodeIndex> {
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
    pub fn get_getter_builder(&self, node: NodeIndex) -> Option<RemoteGetterBuilder> {
        self.find_getter_for(node).map(|r| {
            let domain = self.ingredients[r].domain();
            let shards = (0..self.domains[&domain].shards())
                .map(|i| match self.domains[&domain].assignment(i) {
                    Some(worker) => self.read_addrs[&worker].clone(),
                    None => self.read_listen_addr.clone(),
                })
                .collect();

            RemoteGetterBuilder { node: r, shards }
        })
    }

    /// Obtain a MutatorBuild that can be used to construct a Mutator to perform writes and deletes
    /// from the given base node.
    pub fn get_mutator_builder(&self, base: NodeIndex) -> MutatorBuilder {
        let node = &self.ingredients[base];

        trace!(self.log, "creating mutator"; "for" => base.index());

        let mut key = self.ingredients[base]
            .suggest_indexes(base)
            .remove(&base)
            .map(|(c, _)| c)
            .unwrap_or_else(Vec::new);
        let mut is_primary = false;
        if key.is_empty() {
            if let Sharding::ByColumn(col) = self.ingredients[base].sharded_by() {
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
    pub fn get_statistics(&mut self, _: ()) -> GraphStats {
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

    pub fn install_recipe(&mut self, r_txt: String) {
        self.migrate(|mig| {
            use Recipe;
            let mut r = Recipe::from_str(&r_txt, None).unwrap();
            assert!(r.activate(mig, false).is_ok());
        });
    }

    #[cfg(test)]
    pub fn get_mutator(&self, base: NodeIndex) -> Mutator {
        self.get_mutator_builder(base)
            .build("127.0.0.1:0".parse().unwrap())
    }
    #[cfg(test)]
    pub fn get_getter(&self, node: NodeIndex) -> Option<RemoteGetter> {
        self.get_getter_builder(node).map(|g| g.build())
    }
}

// Using this format trait will omit egress and sharder nodes.
impl fmt::LowerHex for ControllerInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use petgraph::visit::EdgeRef;
        let indentln = |f: &mut fmt::Formatter| write!(f, "    ");

        // Output header.
        writeln!(f, "digraph {{")?;

        // Output global formatting.
        indentln(f)?;
        writeln!(f, "node [shape=record, fontsize=10]")?;

        // Output node descriptions.

        let mut nodes = Vec::new();
        nodes.push(self.source);

        let mut visited = HashSet::new();
        while !nodes.is_empty() {
            let n = nodes.pop().unwrap();
            if visited.contains(&n) {
                continue;
            }
            if self.ingredients[n].is_internal() {
                indentln(f)?;
                write!(f, "{}", n.index())?;
                self.ingredients[n].describe(f, n)?;
            }

            let mut targets: Vec<NodeIndex> =
                self.ingredients.edges(n).map(|e| e.target()).collect();
            let mut propagated_targets = HashSet::new();

            while !targets.is_empty() {
                let node = targets.pop().unwrap();
                if !self.ingredients[node].is_internal() {
                    for e in self.ingredients.edges(node) {
                        targets.push(e.target());
                    }
                } else {
                    propagated_targets.insert(node);
                }
            }

            for target in propagated_targets {
                indentln(f)?;
                write!(f, "{} -> {}", n.index(), target.index())?;
                writeln!(f, "")?;
                nodes.push(target);
            }

            visited.insert(n);
        }

        // Output footer.
        write!(f, "}}")?;

        Ok(())
    }
}

impl fmt::Display for ControllerInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let indentln = |f: &mut fmt::Formatter| write!(f, "    ");

        // Output header.
        writeln!(f, "digraph {{")?;

        // Output global formatting.
        indentln(f)?;
        writeln!(f, "node [shape=record, fontsize=10]")?;

        // Output node descriptions.
        for index in self.ingredients.node_indices() {
            indentln(f)?;
            write!(f, "{}", index.index())?;
            self.ingredients[index].describe(f, index)?;
        }

        // Output edges.
        for (_, edge) in self.ingredients.raw_edges().iter().enumerate() {
            indentln(f)?;
            write!(f, "{} -> {}", edge.source().index(), edge.target().index())?;
            writeln!(f, "")?;
        }

        // Output footer.
        write!(f, "}}")?;

        Ok(())
    }
}

enum ColumnChange {
    Add(String, DataType),
    Drop(usize),
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration<'a> {
    mainline: &'a mut ControllerInner,
    added: Vec<NodeIndex>,
    columns: Vec<(NodeIndex, ColumnChange)>,
    readers: HashMap<NodeIndex, NodeIndex>,

    /// Additional migration information provided by the client
    context: HashMap<String, DataType>,

    start: time::Instant,
    log: slog::Logger,
}

impl<'a> Migration<'a> {
    /// Add the given `Ingredient` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, mut i: I) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
        I: Ingredient + Into<NodeOperator>,
    {
        i.on_connected(&self.mainline.ingredients);
        let parents = i.ancestors();

        let transactional = !parents.is_empty()
            && parents
                .iter()
                .all(|&p| self.mainline.ingredients[p].is_transactional());

        // add to the graph
        let ni = self.mainline.ingredients.add_node(node::Node::new(
            name.to_string(),
            fields,
            i.into(),
            transactional,
        ));
        info!(self.log,
              "adding new node";
              "node" => ni.index(),
              "type" => format!("{:?}", self.mainline.ingredients[ni])
        );

        // keep track of the fact that it's new
        self.added.push(ni);
        // insert it into the graph
        if parents.is_empty() {
            self.mainline
                .ingredients
                .add_edge(self.mainline.source, ni, ());
        } else {
            for parent in parents {
                self.mainline.ingredients.add_edge(parent, ni, ());
            }
        }
        // and tell the caller its id
        ni.into()
    }

    /// Returns the context of this migration
    pub fn context(&self) -> HashMap<String, DataType> {
        self.context.clone()
    }

    /// Returns the universe in which this migration is operating in.
    /// If not specified, assumes `global` universe.
    pub fn universe(&self) -> DataType {
        match self.context.get("id") {
            Some(id) => id.clone(),
            None => "global".into(),
        }
    }

    /// Add a transactional base node to the graph
    pub fn add_transactional_base<S1, FS, S2>(
        &mut self,
        name: S1,
        fields: FS,
        mut b: Base,
    ) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
    {
        b.on_connected(&self.mainline.ingredients);
        let b: NodeOperator = b.into();

        // add to the graph
        let ni = self.mainline
            .ingredients
            .add_node(node::Node::new(name.to_string(), fields, b, true));
        info!(self.log,
              "adding new node";
              "node" => ni.index(),
              "type" => format!("{:?}", self.mainline.ingredients[ni])
        );

        // keep track of the fact that it's new
        self.added.push(ni);
        // insert it into the graph
        self.mainline
            .ingredients
            .add_edge(self.mainline.source, ni, ());
        // and tell the caller its id
        ni.into()
    }

    /// Add a new column to a base node.
    ///
    /// Note that a default value must be provided such that old writes can be converted into this
    /// new type.
    pub fn add_column<S: ToString>(
        &mut self,
        node: NodeIndex,
        field: S,
        default: DataType,
    ) -> usize {
        // not allowed to add columns to new nodes
        assert!(!self.added.iter().any(|&ni| ni == node));

        let field = field.to_string();
        let base = &mut self.mainline.ingredients[node];
        assert!(base.is_internal() && base.get_base().is_some());

        // we need to tell the base about its new column and its default, so that old writes that
        // do not have it get the additional value added to them.
        let col_i1 = base.add_column(&field);
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        {
            let col_i2 = base.inner_mut()
                .get_base_mut()
                .unwrap()
                .add_column(default.clone());
            assert_eq!(col_i1, col_i2);
        }

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Add(field, default)));

        col_i1
    }

    /// Drop a column from a base node.
    pub fn drop_column(&mut self, node: NodeIndex, column: usize) {
        // not allowed to drop columns from new nodes
        assert!(!self.added.iter().any(|&ni| ni == node));

        let base = &mut self.mainline.ingredients[node];
        assert!(base.is_internal() && base.get_base().is_some());

        // we need to tell the base about the dropped column, so that old writes that contain that
        // column will have it filled in with default values (this is done in Mutator).
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        base.inner_mut().get_base_mut().unwrap().drop_column(column);

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Drop(column)));
    }

    #[cfg(test)]
    pub fn graph(&self) -> &Graph {
        self.mainline.graph()
    }

    fn ensure_reader_for(&mut self, n: NodeIndex, name: Option<String>) {
        if !self.readers.contains_key(&n) {
            // make a reader
            let r = node::special::Reader::new(n);
            let r = if let Some(name) = name {
                self.mainline.ingredients[n].named_mirror(r, name)
            } else {
                self.mainline.ingredients[n].mirror(r)
            };
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(n, r, ());
            self.readers.insert(n, r);
        }
    }

    fn ensure_token_generator(&mut self, n: NodeIndex, key: usize) {
        let ri = self.readers[&n];
        if self.mainline.ingredients[ri]
            .with_reader(|r| r.token_generator().is_some())
            .expect("tried to add token generator to non-reader node")
        {
            return;
        }

        // A map from base node to the column in that base node whose value must match the value of
        // this node's column to cause a conflict. Is None for a given base node if any write to
        // that base node might cause a conflict.
        let base_columns: Vec<(_, Option<_>)> =
            keys::provenance_of(&self.mainline.ingredients, n, key, |_, _, _| None)
                .into_iter()
                .map(|path| {
                    // we want the base node corresponding to each path
                    path.into_iter().last().unwrap()
                })
                .collect();

        let coarse_parents = base_columns
            .iter()
            .filter_map(|&(ni, o)| if o.is_none() { Some(ni) } else { None })
            .collect();

        let granular_parents = base_columns
            .into_iter()
            .filter_map(|(ni, o)| {
                if o.is_some() {
                    Some((ni, o.unwrap()))
                } else {
                    None
                }
            })
            .collect();

        let token_generator = checktable::TokenGenerator::new(coarse_parents, granular_parents);
        self.mainline
            .checktable
            .track(token_generator.clone())
            .unwrap();

        self.mainline.ingredients[ri].with_reader_mut(|r| {
            r.set_token_generator(token_generator);
        });
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter` or
    /// `ControllerInner::get_transactional_getter`
    #[cfg(test)]
    pub fn maintain_anonymous(&mut self, n: NodeIndex, key: usize) {
        self.ensure_reader_for(n, None);
        if self.mainline.ingredients[n].is_transactional() {
            self.ensure_token_generator(n, key);
        }

        let ri = self.readers[&n];

        self.mainline.ingredients[ri].with_reader_mut(|r| r.set_key(key));
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter` or
    /// `ControllerInner::get_transactional_getter`
    pub fn maintain(&mut self, name: String, n: NodeIndex, key: usize) {
        self.ensure_reader_for(n, Some(name));
        if self.mainline.ingredients[n].is_transactional() {
            self.ensure_token_generator(n, key);
        }

        let ri = self.readers[&n];

        self.mainline.ingredients[ri].with_reader_mut(|r| r.set_key(key));
    }

    /// Obtain a channel that is fed by the output stream of the given node.
    ///
    /// As new updates are processed by the given node, its outputs will be streamed to the
    /// returned channel. Node that this channel is *not* bounded, and thus a receiver that is
    /// slower than the system as a hole will accumulate a large buffer over time.
    pub fn stream(&mut self, n: NodeIndex) -> mpsc::Receiver<Vec<node::StreamUpdate>> {
        self.ensure_reader_for(n, None);
        let (tx, rx) = mpsc::channel();
        let mut tx = channel::StreamSender::from_local(tx);

        // If the reader hasn't been incorporated into the graph yet, just add the streamer
        // directly.
        let ri = self.readers[&n];
        let mut res = None;
        self.mainline.ingredients[ri].with_reader_mut(|r| {
            res = Some(r.add_streamer(tx));
        });
        tx = match res.unwrap() {
            Ok(_) => return rx,
            Err(tx) => tx,
        };


        // Otherwise, send a message to the reader's domain to have it add the streamer.
        let reader = &self.mainline.ingredients[self.readers[&n]];
        self.mainline
            .domains
            .get_mut(&reader.domain())
            .unwrap()
            .send(box payload::Packet::AddStreamer {
                node: *reader.local_addr(),
                new_streamer: tx,
            })
            .unwrap();

        rx
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(self) {
        info!(self.log, "finalizing migration"; "#nodes" => self.added.len());

        let log = self.log;
        let start = self.start;
        let mut mainline = self.mainline;
        let mut new: HashSet<_> = self.added.into_iter().collect();

        // Readers are nodes too.
        for (_parent, reader) in self.readers {
            new.insert(reader);
        }

        // Shard the graph as desired
        let mut swapped0 = if mainline.sharding_enabled {
            migrate::sharding::shard(&log, &mut mainline.ingredients, mainline.source, &mut new)
        } else {
            HashMap::default()
        };

        // Assign domains
        migrate::assignment::assign(
            &log,
            &mut mainline.ingredients,
            mainline.source,
            &new,
            &mut mainline.ndomains,
        );

        // Set up ingress and egress nodes
        let swapped1 =
            migrate::routing::add(&log, &mut mainline.ingredients, mainline.source, &mut new);

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
                        if mainline.ingredients.find_edge(src, instead).is_some() {
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
        let swapped = swapped0;

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<DomainIndex> = new.iter()
            .filter(|&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&ni| mainline.ingredients[ni].domain())
            .collect();

        let mut domain_new_nodes = new.iter()
            .filter(|&&ni| ni != mainline.source)
            .filter(|&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&ni| (mainline.ingredients[ni].domain(), ni))
            .fold(HashMap::new(), |mut dns, (d, ni)| {
                dns.entry(d).or_insert_with(Vec::new).push(ni);
                dns
            });

        // Assign local addresses to all new nodes, and initialize them
        for (domain, nodes) in &mut domain_new_nodes {
            // Number of pre-existing nodes
            let mut nnodes = mainline.remap.get(domain).map(HashMap::len).unwrap_or(0);

            if nodes.is_empty() {
                // Nothing to do here
                continue;
            }

            let log = log.new(o!("domain" => domain.index()));

            // Give local addresses to every (new) node
            for &ni in nodes.iter() {
                debug!(log,
                       "assigning local index";
                       "type" => format!("{:?}", mainline.ingredients[ni]),
                       "node" => ni.index(),
                       "local" => nnodes
                );

                let mut ip: IndexPair = ni.into();
                ip.set_local(unsafe { LocalNodeIndex::make(nnodes as u32) });
                mainline.ingredients[ni].set_finalized_addr(ip);
                mainline
                    .remap
                    .entry(*domain)
                    .or_insert_with(HashMap::new)
                    .insert(ni, ip);
                nnodes += 1;
            }

            // Initialize each new node
            for &ni in nodes.iter() {
                if mainline.ingredients[ni].is_internal() {
                    // Figure out all the remappings that have happened
                    // NOTE: this has to be *per node*, since a shared parent may be remapped
                    // differently to different children (due to sharding for example). we just
                    // allocate it once though.
                    let mut remap = mainline.remap[domain].clone();

                    // Parents in other domains have been swapped for ingress nodes.
                    // Those ingress nodes' indices are now local.
                    for (&(dst, src), &instead) in &swapped {
                        if dst != ni {
                            // ignore mappings for other nodes
                            continue;
                        }

                        let old = remap.insert(src, mainline.remap[domain][&instead]);
                        assert_eq!(old, None);
                    }

                    trace!(log, "initializing new node"; "node" => ni.index());
                    mainline
                        .ingredients
                        .node_weight_mut(ni)
                        .unwrap()
                        .on_commit(&remap);
                }
            }
        }

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
        // println!("{}", mainline);

        let new_deps = migrate::transactions::analyze_changes(
            &mainline.ingredients,
            mainline.source,
            domain_new_nodes,
        );

        migrate::transactions::merge_deps(&mainline.ingredients, &mut mainline.deps, new_deps);

        let mut uninformed_domain_nodes = mainline
            .ingredients
            .node_indices()
            .filter(|&ni| ni != mainline.source)
            .filter(|&ni| !mainline.ingredients[ni].is_dropped())
            .map(|ni| {
                (mainline.ingredients[ni].domain(), ni, new.contains(&ni))
            })
            .fold(HashMap::new(), |mut dns, (d, ni, new)| {
                dns.entry(d).or_insert_with(Vec::new).push((ni, new));
                dns
            });

        let (start_ts, end_ts, prevs) = mainline
            .checktable
            .perform_migration(mainline.deps.clone())
            .unwrap();

        info!(log, "migration claimed timestamp range"; "start" => start_ts, "end" => end_ts);

        let mut workers: Vec<_> = mainline
            .workers
            .values()
            .map(|w| w.sender.clone())
            .collect();
        let placer_workers: Vec<_> = mainline
            .workers
            .iter()
            .map(|(id, status)| (id.clone(), status.sender.clone()))
            .collect();
        let mut placer: Box<Iterator<Item = (WorkerIdentifier, WorkerEndpoint)>> =
            Box::new(placer_workers.into_iter().cycle());

        // Boot up new domains (they'll ignore all updates for now)
        debug!(log, "booting new domains");
        for domain in changed_domains {
            if mainline.domains.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            let nodes = uninformed_domain_nodes.remove(&domain).unwrap();
            let d = DomainHandle::new(
                domain,
                mainline.ingredients[nodes[0].0].sharded_by(),
                &log,
                &mut mainline.ingredients,
                &mainline.readers,
                &mainline.domain_config,
                nodes,
                &mainline.persistence,
                &mainline.listen_addr,
                &mainline.checktable_addr,
                &mainline.channel_coordinator,
                &mainline.debug_channel,
                &mut placer,
                &mut workers,
                start_ts,
            );
            mainline.domains.insert(domain, d);
        }


        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        debug!(log, "mutating existing domains");
        migrate::augmentation::inform(
            &log,
            &mut mainline,
            uninformed_domain_nodes,
            start_ts,
            prevs.unwrap(),
        );

        // Tell all base nodes about newly added columns
        for (ni, change) in self.columns {
            let n = &mainline.ingredients[ni];
            let m = match change {
                ColumnChange::Add(field, default) => box payload::Packet::AddBaseColumn {
                    node: *n.local_addr(),
                    field: field,
                    default: default,
                },
                ColumnChange::Drop(column) => box payload::Packet::DropBaseColumn {
                    node: *n.local_addr(),
                    column: column,
                },
            };

            let domain = mainline.domains.get_mut(&n.domain()).unwrap();

            domain.send(m).unwrap();
            domain.wait_for_ack().unwrap();
        }

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        info!(log, "bringing up inter-domain connections");
        migrate::routing::connect(&log, &mut mainline.ingredients, &mut mainline.domains, &new);

        // And now, the last piece of the puzzle -- set up materializations
        info!(log, "initializing new materializations");
        mainline
            .materializations
            .commit(&mainline.ingredients, &new, &mut mainline.domains);

        info!(log, "finalizing migration");

        // Ideally this should happen as part of checktable::perform_migration(), but we don't know
        // the replay paths then. It is harmless to do now since we know the new replay paths won't
        // request timestamps until after the migration in finished.
        mainline
            .checktable
            .add_replay_paths(mainline.materializations.domains_on_path.clone())
            .unwrap();

        migrate::transactions::finalize(mainline.deps.clone(), &log, &mut mainline.domains, end_ts);

        warn!(log, "migration completed"; "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
    }
}

impl Drop for ControllerInner {
    fn drop(&mut self) {
        for (_, d) in &mut self.domains {
            // don't unwrap, because given domain may already have terminated
            drop(d.send(box payload::Packet::Quit));
        }
        for (_, mut d) in self.domains.drain() {
            d.wait();
        }
    }
}

/// `Blender` is a handle to a Controller.
pub struct Blender {
    url: String,
}
impl Blender {
    fn rpc<Q: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        request: &Q,
    ) -> Result<R, Box<Error>> {
        use hyper;

        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        let url = format!("{}/{}", self.url, path);

        let mut r = hyper::Request::new(hyper::Method::Post, url.parse().unwrap());
        r.set_body(serde_json::to_string(request).unwrap());

        let work = client.request(r).and_then(|res| {
            res.body().concat2().and_then(move |body| {
                let reply: R = serde_json::from_slice(&body)
                    .map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::Other, e))
                    .unwrap();
                Ok(reply)
            })
        });
        Ok(core.run(work).unwrap())
    }

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Base`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    pub fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.rpc("inputs", &()).unwrap()
    }

    /// Get a Vec of to all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    pub fn outputs(&self) -> BTreeMap<String, NodeIndex> {
        self.rpc("outputs", &()).unwrap()
    }

    /// Obtain a `RemoteGetterBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node.
    pub fn get_getter_builder(&self, node: NodeIndex) -> Option<RemoteGetterBuilder> {
        self.rpc("getter_builder", &node).unwrap()
    }

    /// Obtain a `RemoteGetter`.
    pub fn get_getter(&self, node: NodeIndex) -> Option<RemoteGetter> {
        self.get_getter_builder(node).map(|g| g.build())
    }

    /// Obtain a MutatorBuild that can be used to construct a Mutator to perform writes and deletes
    /// from the given base node.
    pub fn get_mutator_builder(&self, base: NodeIndex) -> Result<MutatorBuilder, Box<Error>> {
        self.rpc("mutator_builder", &base)
    }

    /// Obtain a Mutator
    pub fn get_mutator(&self, base: NodeIndex) -> Result<Mutator, Box<Error>> {
        self.get_mutator_builder(base)
            .map(|m| m.build("127.0.0.1:0".parse().unwrap()))
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn get_statistics(&mut self) -> GraphStats {
        self.rpc("get_statistics", &()).unwrap()
    }

    /// Install a new recipe on the controller.
    pub fn install_recipe(&self, new_recipe: String) {
        self.rpc("install_recipe", &new_recipe).unwrap()
    }

    /// Set the `Logger` to use for internal log messages.
    pub fn log_with(&mut self, _: slog::Logger) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    fn it_works_default() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        ControllerBuilder::default().build();
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration() {
        // use Recipe;

        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";
        // let mut r = Recipe::from_str(r_txt, None).unwrap();

        let c = ControllerBuilder::default().build();
        c.install_recipe(r_txt.to_owned());
        // b.migrate(|mig| {
        //     assert!(r.activate(mig, false).is_ok());
        // });
    }
}
