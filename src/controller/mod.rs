use channel::poll::{PollEvent, PollingLoop, ProcessResult};
use channel::tcp::TcpSender;
use channel;
use consensus::{Authority, Epoch};
use dataflow::prelude::*;
use dataflow::checktable::service::CheckTableServer;
use dataflow::{checktable, node, payload, DomainConfig, PersistenceParameters};
use dataflow::ops::base::Base;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Receiver, Sender};
use std::{io, time};

use futures::{self, Future, Stream};
use hyper::{self, Method, StatusCode};
use hyper::header::ContentType;
use hyper::server::{Http, NewService, Request, Response, Service};
use petgraph;
use rand;
use serde::Serialize;
use serde_json;
use slog;

pub mod domain_handle;
pub mod keys;
pub mod migrate;

pub(crate) mod recipe;
pub(crate) mod sql;

mod builder;
mod getter;
mod handle;
mod inner;
mod mir_to_flow;
mod mutator;

use self::domain_handle::DomainHandle;
use coordination::{CoordinationMessage};
use self::inner::ControllerInner;

pub use self::builder::ControllerBuilder;
pub use self::handle::ControllerHandle;
pub use self::mutator::{Mutator, MutatorBuilder, MutatorError};
pub use self::getter::{Getter, ReadQuery, ReadReply, RemoteGetter, RemoteGetterBuilder};
pub(crate) use self::getter::LocalOrNot;
use self::recipe::Recipe;

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

#[derive(Clone)]
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
        let ni =
            self.mainline
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
    #[allow(unused)]
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
        let mut swapped0 = if let Some(shards) = mainline.sharding {
            migrate::sharding::shard(
                &log,
                &mut mainline.ingredients,
                mainline.source,
                &mut new,
                shards,
            )
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
        let mut sorted_new = new.iter().collect::<Vec<_>>();
        sorted_new.sort();

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<DomainIndex> = sorted_new
            .iter()
            .filter(|&&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&&ni| mainline.ingredients[ni].domain())
            .collect();

        let mut domain_new_nodes = sorted_new
            .iter()
            .filter(|&&&ni| ni != mainline.source)
            .filter(|&&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&&ni| (mainline.ingredients[ni].domain(), ni))
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
            .map(|ni| (mainline.ingredients[ni].domain(), ni, new.contains(&ni)))
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
                &mainline.channel_coordinator,
                &mut mainline.local_pool,
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

        // Tell all base nodes and base ingress children about newly added columns
        for (ni, change) in self.columns {
            let mut inform = if let ColumnChange::Add(..) = change {
                // we need to inform all of the base's children too,
                // so that they know to add columns to existing records when replaying
                mainline
                    .ingredients
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&eni| mainline.ingredients[eni].is_egress())
                    .flat_map(|eni| {
                        // find ingresses under this egress
                        mainline
                            .ingredients
                            .neighbors_directed(eni, petgraph::EdgeDirection::Outgoing)
                    })
                    .collect()
            } else {
                // ingress nodes don't need to know about deleted columns, because those are only
                // relevant when new writes enter the graph.
                Vec::new()
            };
            inform.push(ni);

            for ni in inform {
                let n = &mainline.ingredients[ni];
                let m = match change.clone() {
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

/// Wrapper around the state of a thread serving network requests. Both tracks the address that the
/// thread is listening on and provides a way to stop it.
struct ServingThread {
    addr: SocketAddr,
    join_handle: JoinHandle<()>,
    stop: Box<Drop + Send>,
}
impl ServingThread {
    fn stop(self) {
        drop(self.stop);
        self.join_handle.join().unwrap();
    }
}

/// Describes a running controller instance. A serialized version of this struct is stored in
/// ZooKeeper so that clients can reach the currently active controller.
#[derive(Serialize, Deserialize)]
struct ControllerDescriptor {
    external_addr: SocketAddr,
    internal_addr: SocketAddr,
    checktable_addr: SocketAddr,
    nonce: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct ControllerConfig {
    sharding: Option<usize>,
    partial_enabled: bool,
    domain_config: DomainConfig,
    persistence: PersistenceParameters,
    heartbeat_every: Duration,
    healthcheck_every: Duration,
    local_workers: usize,
    nworkers: usize,
}
impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            sharding: Some(2),
            #[cfg(not(test))]
            sharding: None,
            partial_enabled: true,
            domain_config: DomainConfig {
                concurrent_replays: 512,
                replay_batch_timeout: time::Duration::from_millis(1),
                replay_batch_size: 32,
            },
            persistence: Default::default(),
            heartbeat_every: Duration::from_secs(1),
            healthcheck_every: Duration::from_secs(10),
            #[cfg(test)]
            local_workers: 2,
            #[cfg(not(test))]
            local_workers: 0,
            nworkers: 0,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ControllerState {
    config: ControllerConfig,
    epoch: Epoch,
    recipe: (), // TODO: store all relevant state here.
}

enum ControlEvent {
    ControllerMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<String, StatusCode>>,
    ),
    WonLeaderElection(ControllerState),
    LostLeadership(Epoch),
    Shutdown,
    Error(Box<Error + Send + Sync>),
}

/// Runs the soup instance.
pub struct Controller<A: Authority + 'static> {
    current_epoch: Option<Epoch>,
    inner: Option<ControllerInner>,
    log: slog::Logger,

    listen_addr: IpAddr,
    internal: ServingThread,
    external: ServingThread,
    checktable: SocketAddr,
    _campaign: JoinHandle<()>,

    phantom: PhantomData<A>,
}

impl<A: Authority + 'static> Controller<A> {
    /// Start up a new `Controller` and return a handle to it. Dropping the handle will stop the
    /// controller.
    fn start(
        authority: A,
        listen_addr: IpAddr,
        config: ControllerConfig,
        log: slog::Logger,
    ) -> ControllerHandle<A> {
        let authority = Arc::new(authority);

        let (event_tx, event_rx) = mpsc::channel();
        let internal = Self::listen_internal(event_tx.clone(), SocketAddr::new(listen_addr, 0));
        let checktable = CheckTableServer::start(SocketAddr::new(listen_addr, 0));
        let external = Self::listen_external(
            event_tx.clone(),
            SocketAddr::new(listen_addr, 9000),
            authority.clone(),
        );

        let descriptor = ControllerDescriptor {
            external_addr: external.addr,
            internal_addr: internal.addr,
            checktable_addr: checktable,
            nonce: rand::random(),
        };
        let campaign = Self::campaign(event_tx.clone(), authority.clone(), descriptor, config);

        let builder = thread::Builder::new().name("srv-main".to_owned());
        let join_handle = builder
            .spawn(move || {
                let controller = Self {
                    current_epoch: None,
                    inner: None,
                    log,
                    internal,
                    external,
                    checktable,
                    _campaign: campaign,
                    listen_addr,
                    phantom: PhantomData,
                };
                controller.main_loop(event_rx)
            })
            .unwrap();

        ControllerHandle {
            url: None,
            authority,
            local: Some((event_tx, join_handle)),
        }
    }

    fn main_loop(mut self, receiver: Receiver<ControlEvent>) {
        for event in receiver {
            match event {
                ControlEvent::ControllerMessage(msg) => if let Some(ref mut inner) = self.inner {
                    inner.coordination_message(msg)
                },
                ControlEvent::ExternalRequest(method, path, body, reply_tx) => {
                    if let Some(ref mut inner) = self.inner {
                        reply_tx
                            .send(inner.external_request(method, path, body))
                            .unwrap()
                    } else {
                        reply_tx.send(Err(StatusCode::NotFound)).unwrap();
                    }
                }
                ControlEvent::WonLeaderElection(state) => {
                    self.current_epoch = Some(state.epoch);
                    self.inner = Some(ControllerInner::new(
                        self.listen_addr,
                        self.checktable,
                        self.log.clone(),
                        state,
                    ));
                }
                ControlEvent::LostLeadership(new_epoch) => {
                    self.current_epoch = Some(new_epoch);
                    self.inner = None;
                }
                ControlEvent::Shutdown => break,
                ControlEvent::Error(e) => panic!("{}", e),
            }
        }
        self.external.stop();
        self.internal.stop();
    }

    fn listen_external(
        event_tx: Sender<ControlEvent>,
        addr: SocketAddr,
        authority: Arc<A>,
    ) -> ServingThread {
        struct ExternalServer<A: Authority>(Sender<ControlEvent>, Arc<A>);
        impl<A: Authority> Clone for ExternalServer<A> {
            // Needed due to #26925
            fn clone(&self) -> Self {
                ExternalServer(self.0.clone(), self.1.clone())
            }
        }
        impl<A: Authority> Service for ExternalServer<A> {
            type Request = Request;
            type Response = Response;
            type Error = hyper::Error;
            type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

            fn call(&self, req: Request) -> Self::Future {
                let mut res = Response::new();
                match (req.method().clone(), req.path().to_owned().as_ref()) {
                    (Method::Get, "/graph.html") => {
                        res.headers_mut().set(ContentType::html());
                        res.set_body(include_str!("graph.html"));
                        Box::new(futures::future::ok(res))
                    }
                    (Method::Get, "/js/layout-worker.js") => {
                        res.set_body(
                            "importScripts('https://cdn.rawgit.com/mstefaniuk/graph-viz-d3-js/\
                             cf2160ee3ca39b843b081d5231d5d51f1a901617/dist/layout-worker.js');",
                        );
                        Box::new(futures::future::ok(res))
                    }
                    (Method::Get, path) if path.starts_with("/zookeeper/") => {
                        match self.1.try_read(&format!("/{}", &path[11..])) {
                            Ok(Some(data)) => {
                                res.headers_mut().set(ContentType::json());
                                res.set_body(data);
                            }
                            _ => res.set_status(StatusCode::NotFound),
                        }
                        Box::new(futures::future::ok(res))
                    }
                    (method, path) => {
                        let path = path.to_owned();
                        let event_tx = self.0.clone();
                        Box::new(req.body().concat2().and_then(move |body| {
                            let body: Vec<u8> = body.iter().cloned().collect();
                            let (tx, rx) = futures::sync::oneshot::channel();
                            let _ = event_tx.send(ControlEvent::ExternalRequest(
                                method,
                                path,
                                body,
                                tx,
                            ));
                            rx.and_then(|reply| {
                                let mut res = Response::new();
                                match reply {
                                    Ok(reply) => res.set_body(reply),
                                    Err(status_code) => {
                                        res.set_status(status_code);
                                    }
                                }
                                Box::new(futures::future::ok(res))
                            }).or_else(|futures::Canceled| {
                                let mut res = Response::new();
                                res.set_status(StatusCode::NotFound);
                                Box::new(futures::future::ok(res))
                            })
                        }))
                    }
                }
            }
        }
        impl<A: Authority> NewService for ExternalServer<A> {
            type Request = Request;
            type Response = Response;
            type Error = hyper::Error;
            type Instance = Self;
            fn new_service(&self) -> Result<Self::Instance, io::Error> {
                Ok(self.clone())
            }
        }

        let (tx, rx) = mpsc::channel();
        let (done_tx, done_rx) = futures::sync::oneshot::channel();
        let builder = thread::Builder::new().name("srv-ext".to_owned());
        let join_handle = builder
            .spawn(move || {
                let service = ExternalServer(event_tx, authority);
                let server = Http::new().bind(&addr, service.clone());
                let server = match server {
                    Ok(s) => s,
                    Err(hyper::Error::Io(ref e))
                        if e.kind() == ErrorKind::AddrInUse && addr.port() != 0 =>
                    {
                        Http::new()
                            .bind(&SocketAddr::new(addr.ip(), 0), service)
                            .unwrap()
                    }
                    Err(e) => panic!("{}", e),
                };

                let addr = server.local_addr().unwrap();
                tx.send(addr).unwrap();
                server.run_until(done_rx.map_err(|_| ())).unwrap();
            })
            .unwrap();

        ServingThread {
            addr: rx.recv().unwrap(),
            join_handle,
            stop: Box::new(done_tx),
        }
    }

    /// Listen for messages from workers.
    fn listen_internal(event_tx: Sender<ControlEvent>, addr: SocketAddr) -> ServingThread {
        let mut done = Arc::new(());
        let done2 = done.clone();
        let mut pl: PollingLoop<CoordinationMessage> = PollingLoop::new(addr);
        let addr = pl.get_listener_addr().unwrap();
        let builder = thread::Builder::new().name("srv-int".to_owned());
        let join_handle = builder
            .spawn(move || {
                pl.run_polling_loop(move |e| {
                    if Arc::get_mut(&mut done).is_some() {
                        return ProcessResult::StopPolling;
                    }

                    match e {
                        PollEvent::ResumePolling(timeout) => {
                            *timeout = Some(Duration::from_millis(100));
                        }
                        PollEvent::Process(msg) => {
                            if event_tx.send(ControlEvent::ControllerMessage(msg)).is_err() {
                                return ProcessResult::StopPolling;
                            }
                        }
                        PollEvent::Timeout => {}
                    }
                    ProcessResult::KeepPolling
                });
            })
            .unwrap();

        ServingThread {
            addr,
            join_handle,
            stop: Box::new(done2),
        }
    }

    fn campaign(
        event_tx: Sender<ControlEvent>,
        authority: Arc<A>,
        descriptor: ControllerDescriptor,
        config: ControllerConfig,
    ) -> JoinHandle<()> {
        let descriptor = serde_json::to_vec(&descriptor).unwrap();
        let campaign_inner =
            move |event_tx: Sender<ControlEvent>| -> Result<(), Box<Error + Send + Sync>> {
                loop {
                    // become leader
                    let current_epoch = authority.become_leader(descriptor.clone())?;
                    let state = authority.read_modify_write(
                        "/state",
                        |state: Option<ControllerState>| match state {
                            None => Ok(ControllerState {
                                config: config.clone(),
                                epoch: current_epoch,
                                recipe: (),
                            }),
                            Some(ref state) if state.epoch > current_epoch => Err(()),
                            Some(mut state) => {
                                state.epoch = current_epoch;
                                Ok(state)
                            }
                        },
                    )?;
                    if state.is_err() {
                        continue;
                    }
                    if !event_tx
                        .send(ControlEvent::WonLeaderElection(state.unwrap()))
                        .is_ok()
                    {
                        break;
                    }

                    // watch for overthrow
                    let new_epoch = authority.await_new_epoch(current_epoch)?;
                    if !event_tx
                        .send(ControlEvent::LostLeadership(new_epoch))
                        .is_ok()
                    {
                        break;
                    }
                }
                Ok(())
            };

        thread::Builder::new()
            .name("srv-zk".to_owned())
            .spawn(move || {
                if let Err(e) = campaign_inner(event_tx.clone()) {
                    let _ = event_tx.send(ControlEvent::Error(e));
                }
            })
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus::ZookeeperAuthority;

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    #[allow_fail]
    fn it_works_default() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_default");
        {
            let _c = ControllerBuilder::default().build(authority);
            thread::sleep(Duration::from_millis(100));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    #[allow_fail]
    fn it_works_blender_with_migration() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_blender_with_migration");
        let mut c = ControllerBuilder::default().build(authority);
        c.install_recipe(r_txt.to_owned());
    }

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    fn it_works_default_local() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        {
            let _c = ControllerBuilder::default().build_local();
            thread::sleep(Duration::from_millis(100));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration_local() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = ControllerBuilder::default().build_local();
        c.install_recipe(r_txt.to_owned());
    }
}
