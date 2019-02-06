/// Only allow processing this many inputs in a domain before we handle timer events, acks, etc.
const FORCE_INPUT_YIELD_EVERY: usize = 64;

use crate::controller::domain_handle::{DomainHandle, DomainShardHandle};
use crate::controller::inner::ControllerInner;
use crate::controller::recipe::Recipe;
use crate::controller::sql::reuse::ReuseConfigType;
use crate::coordination::{CoordinationMessage, CoordinationPayload, DomainDescriptor};
use async_bincode::{AsyncBincodeReader, AsyncBincodeStream, AsyncBincodeWriter, AsyncDestination};
use bincode;
use bufstream::BufStream;
use dataflow::{
    payload::{ControlReplyPacket, SourceChannelIdentifier},
    prelude::{DataType, Executor},
    Domain, DomainBuilder, DomainConfig, Packet, PersistenceParameters, PollEvent, ProcessResult,
    Readers,
};
use failure::{self, ResultExt};
use fnv::{FnvHashMap, FnvHashSet};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::mpsc::UnboundedSender;
use futures::{self, Future, Sink, Stream};
use hyper::{self, header::CONTENT_TYPE, Method, StatusCode};
use noria::channel::{self, DualTcpStream, TcpSender, CONNECTION_FROM_BASE};
use noria::consensus::{Authority, Epoch, STATE_KEY};
use noria::internal::{DomainIndex, LocalOrNot};
use noria::{ControllerDescriptor, Input, Tagged};
use rand;
use serde_json;
use slog;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{self, Duration};
use stream_cancel::{Trigger, Valve, Valved};
use streamunordered::{StreamUnordered, StreamYield};
use tokio;
use tokio::prelude::future::{poll_fn, Either};
use tokio::prelude::*;
use tokio_io_pool;
use tokio_threadpool::blocking;
use tokio_tower::multiplex::server;
use tower_util::ServiceFn;

#[cfg(test)]
use std::boxed::FnBox;

pub mod domain_handle;
pub mod keys;
pub mod migrate;

pub(crate) mod recipe;
pub(crate) mod security;
pub(crate) mod sql;

mod builder;
mod handle;
mod inner;
mod mir_to_flow;
mod readers;
mod schema;

pub use crate::controller::builder::WorkerBuilder;
pub use crate::controller::handle::{SyncWorkerHandle, WorkerHandle};
pub use crate::controller::migrate::Migration;
pub use noria::builders::*;
pub use noria::prelude::*;

pub(crate) struct Worker {
    pub(crate) healthy: bool,
    last_heartbeat: time::Instant,
    pub(crate) sender: TcpSender<CoordinationMessage>,
}

impl Worker {
    pub fn new(sender: TcpSender<CoordinationMessage>) -> Self {
        Worker {
            healthy: true,
            last_heartbeat: time::Instant::now(),
            sender,
        }
    }
}

type WorkerIdentifier = SocketAddr;

type ReplicaIndex = (DomainIndex, usize);
type ChannelCoordinator = channel::ChannelCoordinator<ReplicaIndex, Box<Packet>>;

fn block_on<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    let mut wrap = Some(f);
    poll_fn(|| blocking(|| wrap.take().unwrap()()))
        .wait()
        .unwrap()
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct WorkerConfig {
    pub sharding: Option<usize>,
    pub partial_enabled: bool,
    pub domain_config: DomainConfig,
    pub persistence: PersistenceParameters,
    pub heartbeat_every: Duration,
    pub healthcheck_every: Duration,
    pub quorum: usize,
    pub reuse: ReuseConfigType,
    pub threads: Option<usize>,
}
impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            sharding: Some(2),
            #[cfg(not(test))]
            sharding: None,
            partial_enabled: true,
            domain_config: DomainConfig {
                concurrent_replays: 512,
                replay_batch_timeout: time::Duration::new(0, 10_000),
            },
            persistence: Default::default(),
            heartbeat_every: Duration::from_secs(1),
            healthcheck_every: Duration::from_secs(10),
            quorum: 1,
            reuse: ReuseConfigType::Finkelstein,
            #[cfg(any(debug_assertions, test))]
            threads: Some(2),
            #[cfg(not(any(debug_assertions, test)))]
            threads: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub config: WorkerConfig,
    pub epoch: Epoch,

    pub recipe_version: usize,
    pub recipes: Vec<String>,
}

enum Event {
    InternalMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Option<String>,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<Result<String, String>, StatusCode>>,
    ),
    LeaderChange(ControllerState, ControllerDescriptor),
    WonLeaderElection(ControllerState),
    CampaignError(failure::Error),
    #[cfg(test)]
    IsReady(futures::sync::oneshot::Sender<bool>),
    #[cfg(test)]
    ManualMigration {
        f: Box<FnBox(&mut Migration) + Send + 'static>,
        done: futures::sync::oneshot::Sender<()>,
    },
}

use std::fmt;
impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Event::InternalMessage(ref cm) => write!(f, "Internal({:?})", cm),
            Event::ExternalRequest(ref m, ref path, ..) => write!(f, "Request({} {})", m, path),
            Event::LeaderChange(..) => write!(f, "LeaderChange(..)"),
            Event::WonLeaderElection(..) => write!(f, "Won(..)"),
            Event::CampaignError(ref e) => write!(f, "CampaignError({:?})", e),
            #[cfg(test)]
            Event::IsReady(..) => write!(f, "IsReady"),
            #[cfg(test)]
            Event::ManualMigration { .. } => write!(f, "ManualMigration{{..}}"),
        }
    }
}

/// Start up a new instance and return a handle to it. Dropping the handle will stop the
/// controller. Make sure that this method is run while on a runtime.
fn start_instance<A: Authority + 'static>(
    authority: Arc<A>,
    listen_addr: IpAddr,
    config: WorkerConfig,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<Duration>,
    log: slog::Logger,
) -> impl Future<Item = WorkerHandle<A>, Error = failure::Error> {
    let mut pool = tokio_io_pool::Builder::default();
    pool.name_prefix("io-worker-");
    if let Some(threads) = config.threads {
        pool.pool_size(threads);
    }
    let iopool = pool.build().unwrap();
    let ioh = iopool.handle().clone();

    let (trigger, valve) = Valve::new();
    let (tx, rx) = futures::sync::mpsc::unbounded();

    let (dtx, drx) = futures::sync::mpsc::unbounded();
    let v = try {
        // we'll be listening for a couple of different types of events:
        // first, events from workers
        let wport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0))?;
        let waddr = wport.local_addr()?;
        // second, messages from the "real world"
        let xport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 6033))
            .or_else(|_| tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0)))?;
        let xaddr = xport.local_addr()?;
        // and third, domain control traffic. this traffic is a little special, since we may need to
        // receive from it while handling control messages (e.g., for replay acks). because of this, we
        // give it its own channel.
        let cport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0))?;
        let caddr = cport.local_addr()?;
        ((wport, waddr), (xport, xaddr), (cport, caddr))
    };
    let ((wport, waddr), (xport, xaddr), (cport, caddr)) = match v {
        Ok(v) => v,
        Err(e) => return future::Either::A(future::err(e)),
    };

    // spawn all of those
    tokio::spawn(listen_internal(&valve, log.clone(), tx.clone(), wport));
    let ext_log = log.clone();
    tokio::spawn(
        listen_external(tx.clone(), valve.wrap(xport.incoming()), authority.clone()).map_err(
            move |e| {
                warn!(ext_log, "external request failed: {:?}", e);
            },
        ),
    );
    tokio::spawn(listen_domain_replies(
        &valve,
        log.clone(),
        dtx.clone(),
        cport,
    ));

    // shared df state
    let coord = Arc::new(ChannelCoordinator::new());

    // note that we do not start up the data-flow until we find a controller!

    let descriptor = ControllerDescriptor {
        external_addr: xaddr,
        worker_addr: waddr,
        domain_addr: caddr,
        nonce: rand::random(),
    };
    let campaign = instance_campaign(tx.clone(), authority.clone(), descriptor, config);

    // set up different loops for the controller "part" and the worker "part" of us. this is
    // necessary because sometimes the two need to communicate (e.g., for migrations), and if they
    // were in a single loop, that could deadlock.
    let (ctrl_tx, ctrl_rx) = futures::sync::mpsc::unbounded();
    let (worker_tx, worker_rx) = futures::sync::mpsc::unbounded();

    // first, a loop that just forwards to the appropriate place
    tokio::spawn(
        rx.map_err(|_| unreachable!())
            .fold((ctrl_tx, worker_tx), move |(ctx, wtx), e| {
                let fw = move |e, to_ctrl| {
                    if to_ctrl {
                        Either::A(ctx.send(e).map(move |ctx| (ctx, wtx)))
                    } else {
                        Either::B(wtx.send(e).map(move |wtx| (ctx, wtx)))
                    }
                };

                match e {
                    Event::InternalMessage(ref msg) => match msg.payload {
                        CoordinationPayload::Deregister => fw(e, true),
                        CoordinationPayload::RemoveDomain => fw(e, false),
                        CoordinationPayload::AssignDomain(..) => fw(e, false),
                        CoordinationPayload::DomainBooted(..) => fw(e, false),
                        CoordinationPayload::Register { .. } => fw(e, true),
                        CoordinationPayload::Heartbeat => fw(e, true),
                        CoordinationPayload::CreateUniverse(..) => fw(e, true),
                    },
                    Event::ExternalRequest(..) => fw(e, true),
                    #[cfg(test)]
                    Event::ManualMigration { .. } => fw(e, true),
                    Event::LeaderChange(..) => fw(e, false),
                    Event::WonLeaderElection(..) => fw(e, true),
                    Event::CampaignError(..) => fw(e, true),
                    #[cfg(test)]
                    Event::IsReady(..) => fw(e, true),
                }
                .map_err(|e| panic!("{:?}", e))
            })
            .map(|_| ()),
    );

    {
        let mut worker_state = InstanceState::Pining;
        let log = log.clone();
        tokio::spawn(
            worker_rx
                .map_err(|_| unreachable!())
                .for_each(move |e| {
                    match e {
                        Event::InternalMessage(msg) => match msg.payload {
                            CoordinationPayload::RemoveDomain => {
                                unimplemented!();
                            }
                            CoordinationPayload::AssignDomain(d) => {
                                if let InstanceState::Active {
                                    epoch,
                                    ref mut add_domain,
                                    ..
                                } = worker_state
                                {
                                    if epoch == msg.epoch {
                                        return Either::A(Box::new(
                                            add_domain.clone().send(d).map(|_| ()).map_err(|d| {
                                                format_err!("could not add new domain {:?}", d)
                                            }),
                                        ));
                                    }
                                } else {
                                    unreachable!();
                                }
                            }
                            CoordinationPayload::DomainBooted(dd) => {
                                if let InstanceState::Active { epoch, .. } = worker_state {
                                    if epoch == msg.epoch {
                                        let domain = dd.domain();
                                        let shard = dd.shard();
                                        let addr = dd.addr();
                                        trace!(
                                            log,
                                            "found that domain {}.{} is at {:?}",
                                            domain.index(),
                                            shard,
                                            addr
                                        );
                                        coord.insert_remote((domain, shard), addr);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        },
                        Event::LeaderChange(state, descriptor) => {
                            if let InstanceState::Active {
                                add_domain,
                                trigger,
                                ..
                            } = worker_state.take()
                            {
                                // XXX: should we wait for current DF to be fully shut down?
                                // FIXME: what about messages in listen_df's ctrl_tx?
                                info!(log, "detected leader change");
                                drop(add_domain);
                                trigger.cancel();
                            } else {
                                info!(log, "found initial leader");
                            }

                            info!(
                                log,
                                "leader listening on external address {:?}",
                                descriptor.external_addr
                            );
                            debug!(
                                log,
                                "leader's worker listen address: {:?}", descriptor.worker_addr
                            );
                            debug!(
                                log,
                                "leader's domain listen address: {:?}", descriptor.domain_addr
                            );

                            // we need to make a new valve that we can use to shut down *just* the
                            // worker in the case of controller failover.
                            let (trigger, valve) = Valve::new();

                            // TODO: memory stuff should probably also be in config?
                            let (rep_tx, rep_rx) = futures::sync::mpsc::unbounded();
                            let ctrl = listen_df(
                                valve,
                                &ioh,
                                log.clone(),
                                (memory_limit, memory_check_frequency),
                                &state,
                                &descriptor,
                                waddr,
                                coord.clone(),
                                listen_addr,
                                rep_rx,
                            );

                            if let Err(e) = ctrl {
                                error!(log, "failed to connect to controller");
                                eprintln!("{:?}", e);
                            } else {
                                // now we can start accepting dataflow messages
                                worker_state = InstanceState::Active {
                                    epoch: state.epoch,
                                    add_domain: rep_tx,
                                    trigger,
                                };
                                warn!(log, "Connected to new leader");
                            }
                        }
                        e => unreachable!("{:?} is not a worker event", e),
                    }
                    Either::B(futures::future::ok(()))
                })
                .and_then(|v| {
                    // shutting down...
                    //
                    // NOTE: the Trigger in InstanceState::Active is dropped when the for_each
                    // closure above is dropped, which will also shut down the worker.
                    //
                    // TODO: maybe flush things or something?
                    Ok(v)
                })
                .map_err(|e| panic!("{:?}", e)),
        );
    }

    {
        let log = log;
        let authority = authority.clone();

        let log2 = log.clone();
        let authority2 = authority.clone();

        // state that this instance will take if it becomes the controller
        let mut campaign = Some(campaign);
        let mut drx = Some(drx);
        tokio::spawn(
            ctrl_rx
                .map_err(|_| unreachable!())
                .fold(None, move |mut controller: Option<ControllerInner>, e| {
                    match e {
                        Event::InternalMessage(msg) => match msg.payload {
                            CoordinationPayload::Deregister => {
                                unimplemented!();
                            }
                            CoordinationPayload::CreateUniverse(universe) => {
                                if let Some(ref mut ctrl) = controller {
                                    block_on(|| ctrl.create_universe(universe).unwrap());
                                }
                            }
                            CoordinationPayload::Register {
                                ref addr,
                                ref read_listen_addr,
                                ..
                            } => {
                                if let Some(ref mut ctrl) = controller {
                                    block_on(|| {
                                        ctrl.handle_register(&msg, addr, read_listen_addr.clone())
                                            .unwrap()
                                    });
                                }
                            }
                            CoordinationPayload::Heartbeat => {
                                if let Some(ref mut ctrl) = controller {
                                    block_on(|| ctrl.handle_heartbeat(&msg).unwrap());
                                }
                            }
                            _ => unreachable!(),
                        },
                        Event::ExternalRequest(method, path, query, body, reply_tx) => {
                            if let Some(ref mut ctrl) = controller {
                                let authority = &authority;
                                let reply = block_on(|| {
                                    ctrl.external_request(method, path, query, body, &authority)
                                });

                                if let Err(_) = reply_tx.send(reply) {
                                    warn!(log, "client hung up");
                                }
                            } else {
                                if let Err(_) = reply_tx.send(Err(StatusCode::NOT_FOUND)) {
                                    warn!(log, "client hung up for 404");
                                }
                            }
                        }
                        #[cfg(test)]
                        Event::ManualMigration { f, done } => {
                            if let Some(ref mut ctrl) = controller {
                                if !ctrl.workers.is_empty() {
                                    block_on(|| {
                                        ctrl.migrate(move |m| f.call_box((m,)));
                                        done.send(()).unwrap();
                                    });
                                }
                            } else {
                                unreachable!("got migration closure before becoming leader");
                            }
                        }
                        #[cfg(test)]
                        Event::IsReady(reply) => {
                            reply
                                .send(
                                    controller
                                        .as_ref()
                                        .map(|ctrl| !ctrl.workers.is_empty())
                                        .unwrap_or(false),
                                )
                                .unwrap();
                        }
                        Event::WonLeaderElection(state) => {
                            let c = campaign.take().unwrap();
                            block_on(move || c.join().unwrap());
                            let drx = drx.take().unwrap();
                            controller =
                                Some(ControllerInner::new(log.clone(), state.clone(), drx));
                        }
                        Event::CampaignError(e) => {
                            panic!("{:?}", e);
                        }
                        e => unreachable!("{:?} is not a controller event", e),
                    }
                    Ok(controller)
                })
                .and_then(move |controller| {
                    // shutting down
                    if controller.is_some() {
                        if let Err(e) = authority2.surrender_leadership() {
                            error!(log2, "failed to surrender leadership");
                            eprintln!("{:?}", e);
                        }
                    }
                    Ok(())
                })
                .map_err(|e| panic!("{:?}", e)),
        );
    }

    future::Either::B(WorkerHandle::new(authority, tx, trigger, iopool))
}

/*
    epoch: state.epoch,
    heartbeat_every: state.config.heartbeat_every,
    evict_every: memory_check_frequency,
    memory_limit,
*/

enum InstanceState {
    Pining,
    Active {
        epoch: Epoch,
        trigger: Trigger,
        add_domain: UnboundedSender<DomainBuilder>,
    },
}

impl InstanceState {
    fn take(&mut self) -> Self {
        ::std::mem::replace(self, InstanceState::Pining)
    }
}

fn listen_reads(
    valve: &Valve,
    ioh: &tokio_io_pool::Handle,
    on: tokio::net::TcpListener,
    readers: Readers,
) -> impl Future<Item = (), Error = ()> {
    ioh.spawn_all(
        valve
            .wrap(on.incoming())
            .map(Some)
            .or_else(|_| {
                // io error from client: just ignore it
                Ok(None)
            })
            .filter_map(|c| c)
            .map(move |stream| {
                let readers = readers.clone();
                stream.set_nodelay(true).expect("could not set TCP_NODELAY");
                server::Server::new(
                    AsyncBincodeStream::from(stream).for_async(),
                    ServiceFn::new(move |req| readers::handle_message(req, &readers)),
                )
                .map_err(|e| -> () {
                    if let server::Error::Service(()) = e {
                        // server is shutting down -- no need to report this error
                    } else {
                        eprintln!("!!! reader client protocol error: {:?}", e);
                    }
                })
            }),
    )
    .map_err(|e: tokio_io_pool::StreamSpawnError<()>| {
        eprintln!(
            "io pool is shutting down, so can't handle more reads: {:?}",
            e
        );
    })
}

fn listen_df(
    valve: Valve,
    ioh: &tokio_io_pool::Handle,
    log: slog::Logger,
    (memory_limit, evict_every): (Option<usize>, Option<Duration>),
    state: &ControllerState,
    desc: &ControllerDescriptor,
    waddr: SocketAddr,
    coord: Arc<ChannelCoordinator>,
    on: IpAddr,
    replicas: futures::sync::mpsc::UnboundedReceiver<DomainBuilder>,
) -> Result<(), failure::Error> {
    // first, try to connect to controller
    let ctrl = ::std::net::TcpStream::connect(&desc.worker_addr)?;
    let ctrl = tokio::net::TcpStream::from_std(ctrl, &Default::default())?;
    let ctrl_addr = ctrl.local_addr()?;
    info!(log, "connected to controller"; "src" => ?ctrl_addr);

    let log_prefix = state.config.persistence.log_prefix.clone();
    let prefix = format!("{}-log-", log_prefix);
    let log_files: Vec<String> = fs::read_dir(".")
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().ok().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.path().to_string_lossy().into_owned())
        .filter(|path| path.starts_with(&prefix))
        .collect();

    // extract important things from state config
    let epoch = state.epoch;
    let heartbeat_every = state.config.heartbeat_every;

    let (ctrl_tx, ctrl_rx) = futures::sync::mpsc::unbounded();

    // reader setup
    let readers = Arc::new(Mutex::new(HashMap::new()));
    let rport = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0))?;
    let raddr = rport.local_addr()?;
    info!(log, "listening for reads"; "on" => ?raddr);

    // start controller message handler
    let ctrl = AsyncBincodeWriter::from(ctrl).for_async();
    tokio::spawn(
        ctrl_rx
            .map(move |cm| CoordinationMessage {
                source: ctrl_addr,
                payload: cm,
                epoch,
            })
            .map_err(|e| panic!("{:?}", e))
            .forward(ctrl.sink_map_err(|e| {
                // if the controller goes away, another will be elected, and the worker will be
                // restarted, so there's no reason to do anything too drastic here.
                eprintln!("controller went away: {:?}", e);
            }))
            .map(|_| ()),
    );

    // also start readers
    tokio::spawn(listen_reads(&valve, ioh, rport, readers.clone()));

    // and tell the controller about us
    let timer = valve.wrap(tokio::timer::Interval::new(
        time::Instant::now() + heartbeat_every,
        heartbeat_every,
    ));
    tokio::spawn(
        ctrl_tx
            .clone()
            .send(CoordinationPayload::Register {
                addr: waddr,
                read_listen_addr: raddr,
                log_files,
            })
            .and_then(move |ctrl_tx| {
                // and start sending heartbeats
                timer
                    .map(|_| CoordinationPayload::Heartbeat)
                    .map_err(|e| -> futures::sync::mpsc::SendError<_> { panic!("{:?}", e) })
                    .forward(ctrl_tx.clone())
                    .map(|_| ())
            })
            .map_err(|_| {
                // we're probably just shutting down
                ()
            }),
    );

    let state_sizes = Arc::new(Mutex::new(HashMap::new()));
    if let Some(evict_every) = evict_every {
        let log = log.clone();
        let mut domain_senders = HashMap::new();
        let state_sizes = state_sizes.clone();
        let timer = valve.wrap(tokio::timer::Interval::new(
            time::Instant::now() + evict_every,
            evict_every,
        ));
        tokio::spawn(
            timer
                .for_each(move |_| {
                    do_eviction(&log, memory_limit, &mut domain_senders, &state_sizes)
                        .map_err(|e| panic!("{:?}", e))
                })
                .map_err(|e| panic!("{:?}", e)),
        );
    }

    // Now we're ready to accept new domains.
    let dcaddr = desc.domain_addr;
    tokio::spawn(
        replicas
            .map_err(|e| -> io::Error { panic!("{:?}", e) })
            .fold(ctrl_tx, move |ctrl_tx, d| {
                let idx = d.index;
                let shard = d.shard.unwrap_or(0);
                let addr: io::Result<_> = try {
                    let on = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0))?;
                    let addr = on.local_addr()?;

                    let state_size = Arc::new(AtomicUsize::new(0));
                    let d = d.build(
                        log.clone(),
                        readers.clone(),
                        coord.clone(),
                        dcaddr,
                        &valve,
                        state_size.clone(),
                    );

                    let (tx, rx) = futures::sync::mpsc::unbounded();

                    // need to register the domain with the local channel coordinator.
                    // local first to ensure that we don't unnecessarily give away remote for a
                    // local thing if there's a race
                    coord.insert_local((idx, shard), tx);
                    coord.insert_remote((idx, shard), addr);

                    block_on(|| state_sizes.lock().unwrap().insert((idx, shard), state_size));

                    tokio::spawn(Replica::new(
                        &valve,
                        d,
                        on,
                        rx,
                        ctrl_tx.clone(),
                        log.clone(),
                        coord.clone(),
                    ));

                    info!(
                        log,
                        "informed controller that domain {}.{} is at {:?}",
                        idx.index(),
                        shard,
                        addr
                    );

                    addr
                };

                match addr {
                    Ok(addr) => Either::A(
                        ctrl_tx
                            .send(CoordinationPayload::DomainBooted(DomainDescriptor::new(
                                idx, shard, addr,
                            )))
                            .map_err(|_| {
                                // controller went away -- exit?
                                io::Error::new(io::ErrorKind::Other, "controller went away")
                            }),
                    ),
                    Err(e) => Either::B(future::err(e)),
                }
            })
            .map_err(|e| panic!("{:?}", e))
            .map(|_| ()),
    );

    Ok(())
}

fn listen_domain_replies(
    valve: &Valve,
    log: slog::Logger,
    reply_tx: UnboundedSender<ControlReplyPacket>,
    on: tokio::net::TcpListener,
) -> impl Future<Item = (), Error = ()> {
    let valve = valve.clone();
    valve
        .wrap(on.incoming())
        .map_err(failure::Error::from)
        .for_each(move |sock| {
            tokio::spawn(
                valve
                    .wrap(AsyncBincodeReader::from(sock))
                    .map_err(failure::Error::from)
                    .forward(
                        reply_tx
                            .clone()
                            .sink_map_err(|_| format_err!("main event loop went away")),
                    )
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e)),
            );
            Ok(())
        })
        .map_err(move |e| {
            warn!(log, "domain reply connection failed: {:?}", e);
        })
}

fn listen_internal(
    valve: &Valve,
    log: slog::Logger,
    event_tx: UnboundedSender<Event>,
    on: tokio::net::TcpListener,
) -> impl Future<Item = (), Error = ()> {
    let valve = valve.clone();
    valve
        .wrap(on.incoming())
        .map_err(failure::Error::from)
        .for_each(move |sock| {
            tokio::spawn(
                valve
                    .wrap(AsyncBincodeReader::from(sock))
                    .map(Event::InternalMessage)
                    .map_err(failure::Error::from)
                    .forward(
                        event_tx
                            .clone()
                            .sink_map_err(|_| format_err!("main event loop went away")),
                    )
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e)),
            );
            Ok(())
        })
        .map_err(move |e| {
            warn!(log, "internal connection failed: {:?}", e);
        })
}

struct ExternalServer<A: Authority>(UnboundedSender<Event>, Arc<A>);
fn listen_external<A: Authority + 'static>(
    event_tx: UnboundedSender<Event>,
    on: Valved<tokio::net::tcp::Incoming>,
    authority: Arc<A>,
) -> impl Future<Item = (), Error = hyper::Error> + Send {
    use hyper::{
        service::{NewService, Service},
        Request, Response,
    };
    impl<A: Authority> Clone for ExternalServer<A> {
        // Needed due to #26925
        fn clone(&self) -> Self {
            ExternalServer(self.0.clone(), self.1.clone())
        }
    }
    impl<A: Authority> Service for ExternalServer<A> {
        type ReqBody = hyper::Body;
        type ResBody = hyper::Body;
        type Error = hyper::Error;
        type Future = Box<Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send>;

        fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
            let mut res = Response::builder();
            // disable CORS to allow use as API server
            res.header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            if let &Method::GET = req.method() {
                match req.uri().path() {
                    "/graph.html" => {
                        res.header(CONTENT_TYPE, "text/html");
                        let res = res.body(hyper::Body::from(include_str!("graph.html")));
                        return Box::new(futures::future::ok(res.unwrap()));
                    }
                    "/js/layout-worker.js" => {
                        let res = res.body(hyper::Body::from(
                            "importScripts('https://cdn.rawgit.com/mstefaniuk/graph-viz-d3-js/\
                             cf2160ee3ca39b843b081d5231d5d51f1a901617/dist/layout-worker.js');",
                        ));
                        return Box::new(futures::future::ok(res.unwrap()));
                    }
                    path if path.starts_with("/zookeeper/") => {
                        let res = match self.1.try_read(&format!("/{}", &path[11..])) {
                            Ok(Some(data)) => {
                                res.header(CONTENT_TYPE, "application/json");
                                res.body(hyper::Body::from(data))
                            }
                            _ => {
                                res.status(StatusCode::NOT_FOUND);
                                res.body(hyper::Body::empty())
                            }
                        };
                        return Box::new(futures::future::ok(res.unwrap()));
                    }
                    _ => {}
                }
            }

            let method = req.method().clone();
            let path = req.uri().path().to_string();
            let query = req.uri().query().map(|s| s.to_owned());
            let event_tx = self.0.clone();
            Box::new(req.into_body().concat2().and_then(move |body| {
                let body: Vec<u8> = body.iter().cloned().collect();
                let (tx, rx) = futures::sync::oneshot::channel();
                event_tx
                    .clone()
                    .send(Event::ExternalRequest(method, path, query, body, tx))
                    .map_err(|_| futures::Canceled)
                    .then(move |_| rx)
                    .then(move |reply| match reply {
                        Ok(reply) => {
                            let res = match reply {
                                Ok(Ok(reply)) => {
                                    res.header("Content-Type", "application/json; charset=utf-8");
                                    res.body(hyper::Body::from(reply))
                                }
                                Ok(Err(reply)) => {
                                    res.status(StatusCode::INTERNAL_SERVER_ERROR);
                                    res.header("Content-Type", "text/plain; charset=utf-8");
                                    res.body(hyper::Body::from(reply))
                                }
                                Err(status_code) => {
                                    res.status(status_code);
                                    res.body(hyper::Body::empty())
                                }
                            };
                            Ok(res.unwrap())
                        }
                        Err(_) => {
                            res.status(StatusCode::NOT_FOUND);
                            Ok(res.body(hyper::Body::empty()).unwrap())
                        }
                    })
            }))
        }
    }
    impl<A: Authority> NewService for ExternalServer<A> {
        type Service = Self;
        type ReqBody = hyper::Body;
        type ResBody = hyper::Body;
        type Error = hyper::Error;
        type InitError = io::Error;
        type Future = tokio::prelude::future::FutureResult<Self, Self::InitError>;
        fn new_service(&self) -> tokio::prelude::future::FutureResult<Self, io::Error> {
            Ok(self.clone()).into()
        }
    }

    let service = ExternalServer(event_tx, authority);
    hyper::server::Server::builder(on).serve(service)
}

fn instance_campaign<A: Authority + 'static>(
    event_tx: UnboundedSender<Event>,
    authority: Arc<A>,
    descriptor: ControllerDescriptor,
    config: WorkerConfig,
) -> JoinHandle<()> {
    let descriptor_bytes = serde_json::to_vec(&descriptor).unwrap();
    let campaign_inner = move |mut event_tx: UnboundedSender<Event>| -> Result<(), failure::Error> {
        let payload_to_event = |payload: Vec<u8>| -> Result<Event, failure::Error> {
            let descriptor: ControllerDescriptor = serde_json::from_slice(&payload[..])?;
            let state: ControllerState =
                serde_json::from_slice(&authority.try_read(STATE_KEY).unwrap().unwrap())?;
            Ok(Event::LeaderChange(state, descriptor))
        };

        loop {
            // WORKER STATE - watch for leadership changes
            //
            // If there is currently a leader, then loop until there is a period without a
            // leader, notifying the main thread every time a leader change occurs.
            let mut epoch;
            if let Some(leader) = authority.try_get_leader()? {
                epoch = leader.0;
                event_tx = event_tx
                    .send(payload_to_event(leader.1)?)
                    .wait()
                    .map_err(|_| format_err!("send failed"))?;
                while let Some(leader) = authority.await_new_epoch(epoch)? {
                    epoch = leader.0;
                    event_tx = event_tx
                        .send(payload_to_event(leader.1)?)
                        .wait()
                        .map_err(|_| format_err!("send failed"))?;
                }
            }

            // ELECTION STATE - attempt to become leader
            //
            // Becoming leader requires creating an ephemeral key and then doing an atomic
            // update to another.
            let epoch = match authority.become_leader(descriptor_bytes.clone())? {
                Some(epoch) => epoch,
                None => continue,
            };
            let state = authority.read_modify_write(
                STATE_KEY,
                |state: Option<ControllerState>| match state {
                    None => Ok(ControllerState {
                        config: config.clone(),
                        epoch,
                        recipe_version: 0,
                        recipes: vec![],
                    }),
                    Some(ref state) if state.epoch > epoch => Err(()),
                    Some(mut state) => {
                        state.epoch = epoch;
                        if state.config != config {
                            panic!("Config in Zk does not match requested config!")
                        }
                        Ok(state)
                    }
                },
            )?;
            if state.is_err() {
                continue;
            }

            // LEADER STATE - manage system
            //
            // It is not currently possible to safely handle involuntary loss of leadership status
            // (and there is nothing that can currently trigger it), so don't bother watching for
            // it.
            break event_tx
                .send(Event::WonLeaderElection(state.clone().unwrap()))
                .and_then(|event_tx| {
                    event_tx.send(Event::LeaderChange(state.unwrap(), descriptor.clone()))
                })
                .wait()
                .map(|_| ())
                .map_err(|_| format_err!("send failed"));
        }
    };

    thread::Builder::new()
        .name("srv-zk".to_owned())
        .spawn(move || {
            if let Err(e) = campaign_inner(event_tx.clone()) {
                let _ = event_tx.send(Event::CampaignError(e));
            }
        })
        .unwrap()
}

fn do_eviction(
    log: &slog::Logger,
    memory_limit: Option<usize>,
    domain_senders: &mut HashMap<(DomainIndex, usize), TcpSender<Box<Packet>>>,
    state_sizes: &Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
) -> impl Future<Item = (), Error = ()> {
    use std::cmp;

    // 2. add current state sizes (could be out of date, as packet sent below is not
    //    necessarily received immediately)
    let sizes: Vec<((DomainIndex, usize), usize)> = block_on(|| {
        let state_sizes = state_sizes.lock().unwrap();
        state_sizes
            .iter()
            .map(|(ds, sa)| {
                let size = sa.load(Ordering::Relaxed);
                trace!(
                    log,
                    "domain {}.{} state size is {} bytes",
                    ds.0.index(),
                    ds.1,
                    size
                );
                (ds.clone(), size)
            })
            .collect()
    });

    // 3. are we above the limit?
    let total: usize = sizes.iter().map(|&(_, s)| s).sum();
    match memory_limit {
        None => (),
        Some(limit) => {
            if total >= limit {
                // evict from the largest domain
                let largest = sizes.into_iter().max_by_key(|&(_, s)| s).unwrap();
                debug!(
                        log,
                        "memory footprint ({} bytes) exceeds limit ({} bytes); evicting from largest domain {}",
                        total,
                        limit,
                        (largest.0).0.index(),
                    );

                let tx = domain_senders.get_mut(&largest.0).unwrap();
                block_on(|| {
                    tx.send(box Packet::Evict {
                        node: None,
                        num_bytes: cmp::min(largest.1, total - limit),
                    })
                    .unwrap()
                });
            }
        }
    }

    Result::Ok::<_, ()>(()).into_future()
}

struct Replica {
    domain: Domain,
    log: slog::Logger,

    coord: Arc<ChannelCoordinator>,

    incoming: Valved<tokio::net::tcp::Incoming>,
    first_byte: FuturesUnordered<tokio::io::ReadExact<tokio::net::tcp::TcpStream, Vec<u8>>>,
    locals: futures::sync::mpsc::UnboundedReceiver<Box<Packet>>,
    inputs: StreamUnordered<
        DualTcpStream<
            BufStream<tokio::net::TcpStream>,
            Box<Packet>,
            Tagged<LocalOrNot<Input>>,
            AsyncDestination,
        >,
    >,
    outputs: FnvHashMap<
        ReplicaIndex,
        (
            Box<dyn Sink<SinkItem = Box<Packet>, SinkError = bincode::Error> + Send>,
            bool,
        ),
    >,

    outbox: FnvHashMap<ReplicaIndex, VecDeque<Box<Packet>>>,
    timeout: Option<tokio::timer::Delay>,
    oob: OutOfBand,
}

impl Replica {
    pub fn new(
        valve: &Valve,
        mut domain: Domain,
        on: tokio::net::TcpListener,
        locals: futures::sync::mpsc::UnboundedReceiver<Box<Packet>>,
        ctrl_tx: futures::sync::mpsc::UnboundedSender<CoordinationPayload>,
        log: slog::Logger,
        cc: Arc<ChannelCoordinator>,
    ) -> Self {
        let id = domain.id();
        let id = format!("{}.{}", id.0.index(), id.1);
        domain.booted(on.local_addr().unwrap());
        Replica {
            coord: cc,
            domain,
            incoming: valve.wrap(on.incoming()),
            first_byte: FuturesUnordered::new(),
            locals,
            log: log.new(o! {"id" => id}),
            inputs: Default::default(),
            outputs: Default::default(),
            outbox: Default::default(),
            oob: OutOfBand::new(ctrl_tx),
            timeout: None,
        }
    }

    fn try_oob(&mut self) -> Result<(), failure::Error> {
        let inputs = &mut self.inputs;
        let pending = &mut self.oob.pending;

        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        self.oob.back.retain(|&streami, tags| {
            let stream = &mut inputs[streami];

            let had = tags.len();
            tags.retain(|&tag| {
                match stream.start_send(Tagged { tag, v: () }) {
                    Ok(AsyncSink::Ready) => false,
                    Ok(AsyncSink::NotReady(_)) => {
                        // TODO: also break?
                        true
                    }
                    Err(e) => {
                        // start_send shouldn't generally error
                        err.push(e.into());
                        true
                    }
                }
            });

            if had != tags.len() {
                pending.insert(streami);
            }

            !tags.is_empty()
        });

        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }

        // then, try to send on any streams we may be able to
        pending.retain(|&streami| {
            let stream = &mut inputs[streami];
            match stream.poll_complete() {
                Ok(Async::Ready(())) => false,
                Ok(Async::NotReady) => true,
                Err(box bincode::ErrorKind::Io(e)) => {
                    match e.kind() {
                        io::ErrorKind::BrokenPipe
                        | io::ErrorKind::NotConnected
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset => {
                            // connection went away, no need to try more
                            false
                        }
                        _ => {
                            err.push(e.into());
                            true
                        }
                    }
                }
                Err(e) => {
                    err.push(e.into());
                    true
                }
            }
        });

        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }

        Ok(())
    }

    fn try_flush(&mut self) -> Result<(), failure::Error> {
        let cc = &self.coord;
        let outputs = &mut self.outputs;

        // just like in try_oob:
        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        for (&ri, ms) in &mut self.outbox {
            if ms.is_empty() {
                continue;
            }

            let &mut (ref mut tx, ref mut pending) = outputs.entry(ri).or_insert_with(|| {
                while !cc.has(&ri) {}
                let tx = cc.builder_for(&ri).unwrap().build_async().unwrap();
                (tx, true)
            });

            while let Some(m) = ms.pop_front() {
                match tx.start_send(m) {
                    Ok(AsyncSink::Ready) => {
                        // we queued something, so we'll need to send!
                        *pending = true;
                    }
                    Ok(AsyncSink::NotReady(m)) => {
                        // put back the m we tried to send
                        ms.push_front(m);
                        // there's also no use in trying to enqueue more packets
                        break;
                    }
                    Err(e) => {
                        err.push(e);
                        break;
                    }
                }
            }
        }

        if !err.is_empty() {
            return Err(err.swap_remove(0).into());
        }

        // then, try to do any sends that are still pending
        for &mut (ref mut tx, ref mut pending) in outputs.values_mut() {
            if !*pending {
                continue;
            }

            match tx.poll_complete() {
                Ok(Async::Ready(())) => {
                    *pending = false;
                }
                Ok(Async::NotReady) => {}
                Err(e) => err.push(e),
            }
        }

        if !err.is_empty() {
            return Err(err.swap_remove(0).into());
        }

        Ok(())
    }

    fn try_new(&mut self) -> io::Result<bool> {
        while let Async::Ready(stream) = self.incoming.poll()? {
            match stream {
                Some(stream) => {
                    // we know that any new connection to a domain will first send a one-byte
                    // token to indicate whether the connection is from a base or not.
                    debug!(self.log, "accepted new connection"; "from" => ?stream.peer_addr().unwrap());
                    self.first_byte
                        .push(tokio::io::read_exact(stream, vec![0; 1]));
                }
                None => {
                    return Ok(false);
                }
            }
        }

        while let Async::Ready(Some((stream, tag))) = self.first_byte.poll()? {
            let is_base = tag[0] == CONNECTION_FROM_BASE;

            debug!(self.log, "established new connection"; "base" => ?is_base);
            let slot = self.inputs.stream_slot();
            let token = slot.token();
            if let Err(e) = stream.set_nodelay(true) {
                warn!(self.log,
                      "failed to set TCP_NODELAY for new connection: {:?}", e;
                      "from" => ?stream.peer_addr().unwrap());
            }
            let tcp = if is_base {
                DualTcpStream::upgrade(BufStream::new(stream), move |Tagged { v: input, tag }| {
                    Box::new(Packet::Input {
                        inner: input,
                        src: Some(SourceChannelIdentifier { token, tag }),
                        senders: Vec::new(),
                    })
                })
            } else {
                BufStream::with_capacities(2 * 1024 * 1024, 4 * 1024, stream).into()
            };
            slot.insert(tcp);
        }
        Ok(true)
    }

    fn try_timeout(&mut self) -> Poll<(), tokio::timer::Error> {
        if let Some(mut to) = self.timeout.take() {
            if let Async::Ready(()) = to.poll()? {
                block_on(|| {
                    self.domain
                        .on_event(&mut self.oob, PollEvent::Timeout, &mut self.outbox)
                });
                return Ok(Async::Ready(()));
            } else {
                self.timeout = Some(to);
            }
        }
        Ok(Async::NotReady)
    }
}

struct OutOfBand {
    // map from inputi to number of (empty) ACKs
    back: FnvHashMap<usize, Vec<u32>>,
    pending: FnvHashSet<usize>,

    // for sending messages to the controller
    ctrl_tx: futures::sync::mpsc::UnboundedSender<CoordinationPayload>,
}

impl OutOfBand {
    fn new(ctrl_tx: futures::sync::mpsc::UnboundedSender<CoordinationPayload>) -> Self {
        OutOfBand {
            back: Default::default(),
            pending: Default::default(),
            ctrl_tx,
        }
    }
}

impl Executor for OutOfBand {
    fn ack(&mut self, id: SourceChannelIdentifier) {
        self.back.entry(id.token).or_default().push(id.tag);
    }

    fn create_universe(&mut self, universe: HashMap<String, DataType>) {
        self.ctrl_tx
            .unbounded_send(CoordinationPayload::CreateUniverse(universe))
            .expect("asked to send to controller, but controller has gone away");
    }
}

impl Future for Replica {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let r: Result<Async<Self::Item>, failure::Error> = try {
            loop {
                // FIXME: check if we should call update_state_sizes (every evict_every)

                // are there are any new connections?
                if !self.try_new().context("check for new connections")? {
                    // incoming socket closed -- no more clients will arrive
                    return Ok(Async::Ready(()));
                }

                // have any of our timers expired?
                self.try_timeout().context("check timeout")?;

                // we have three logical input sources: receives from local domains, receives from
                // remote domains, and remote mutators. we want to achieve some kind of fairness among
                // these, but bias the data-flow towards finishing work it has accepted (i.e., domain
                // operations) to accepting new work. however, this is complicated by two facts:
                //
                //  - we cannot (currently) differentiate between receives from remote domains and
                //    receives from mutators. they are all remote tcp channels. we *could*
                //    differentiate them using `is_base` in `try_new` to store them separately, but
                //    it's also unclear how that would change the receive heuristic.
                //  - domain operations are not all "completing starting work". in many cases, traffic
                //    from domains will be replay-related, in which case favoring domains would favor
                //    writes over reads. while we do in general want reads to be fast, we don't want
                //    them to fully starve writes.
                //
                // the current stategy is therefore that we alternate reading once from the local
                // channel and once from the set of remote channels. this biases slightly in favor of
                // local sends, without starving either. we also stop alternating once either source is
                // depleted.
                let mut local_done = false;
                let mut remote_done = false;
                let mut check_local = true;
                let readiness = loop {
                    let mut interrupted = false;
                    for i in 0..FORCE_INPUT_YIELD_EVERY {
                        if !local_done && (check_local || remote_done) {
                            match self.locals.poll() {
                                Ok(Async::Ready(Some(packet))) => {
                                    let d = &mut self.domain;
                                    let oob = &mut self.oob;
                                    let ob = &mut self.outbox;

                                    if let ProcessResult::StopPolling =
                                        block_on(|| d.on_event(oob, PollEvent::Process(packet), ob))
                                    {
                                        // domain got a message to quit
                                        // TODO: should we finish up remaining work?
                                        return Ok(Async::Ready(()));
                                    }
                                }
                                Ok(Async::Ready(None)) => {
                                    // local input stream finished?
                                    // TODO: should we finish up remaining work?
                                    return Ok(Async::Ready(()));
                                }
                                Ok(Async::NotReady) => {
                                    local_done = true;
                                }
                                Err(e) => {
                                    error!(self.log, "local input stream failed: {:?}", e);
                                    local_done = true;
                                    break;
                                }
                            }
                        }

                        if !remote_done && (!check_local || local_done) {
                            match self.inputs.poll() {
                                Ok(Async::Ready(Some((StreamYield::Item(packet), _)))) => {
                                    let d = &mut self.domain;
                                    let oob = &mut self.oob;
                                    let ob = &mut self.outbox;

                                    if let ProcessResult::StopPolling =
                                        block_on(|| d.on_event(oob, PollEvent::Process(packet), ob))
                                    {
                                        // domain got a message to quit
                                        // TODO: should we finish up remaining work?
                                        return Ok(Async::Ready(()));
                                    }
                                }
                                Ok(Async::Ready(Some((
                                    StreamYield::Finished(_stream),
                                    streami,
                                )))) => {
                                    self.oob.back.remove(&streami);
                                    self.oob.pending.remove(&streami);
                                    // FIXME: what about if a later flush flushes to this stream?
                                }
                                Ok(Async::Ready(None)) => {
                                    // we probably haven't booted yet
                                    remote_done = true;
                                }
                                Ok(Async::NotReady) => {
                                    remote_done = true;
                                }
                                Err(e) => {
                                    error!(self.log, "input stream failed: {:?}", e);
                                    remote_done = true;
                                    break;
                                }
                            }
                        }

                        // alternate between input sources
                        check_local = !check_local;

                        // nothing more to do -- wait to be polled again
                        if local_done && remote_done {
                            break;
                        }

                        if i == FORCE_INPUT_YIELD_EVERY - 1 {
                            // we could keep processing inputs, but make sure we send some ACKs too!
                            interrupted = true;
                        }
                    }

                    // send to downstream
                    // TODO: send fail == exiting?
                    self.try_flush().context("downstream flush (after)")?;

                    // send acks
                    self.try_oob()?;

                    if interrupted {
                        // resume reading from our non-depleted inputs
                        continue;
                    }
                    break Async::NotReady;
                };

                // check if we now need to set a timeout
                match self.domain.on_event(
                    &mut self.oob,
                    PollEvent::ResumePolling,
                    &mut self.outbox,
                ) {
                    ProcessResult::KeepPolling(timeout) => {
                        if let Some(timeout) = timeout {
                            self.timeout =
                                Some(tokio::timer::Delay::new(time::Instant::now() + timeout));

                            // we need to poll the delay to ensure we'll get woken up
                            if let Async::Ready(()) =
                                self.try_timeout().context("check timeout after setting")?
                            {
                                // the timer expired and we did some stuff
                                // make sure we don't return while there's more work to do
                                continue;
                            }
                        }
                    }
                    pr => {
                        // TODO: just have resume_polling be a method...
                        unreachable!("unexpected ResumePolling result {:?}", pr)
                    }
                }

                break readiness;
            }
        };

        match r {
            Ok(k) => Ok(k),
            Err(e) => {
                crit!(self.log, "replica failure: {:?}", e);
                Err(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noria::consensus::ZookeeperAuthority;

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    #[allow_fail]
    fn it_works_default() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_default").unwrap();
        {
            tokio::runtime::current_thread::block_on_all(
                WorkerBuilder::default().start(Arc::new(authority)),
            )
            .unwrap();
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

        use rand::Rng;
        let zk = format!(
            "127.0.0.1:2181/it_works_blender_with_migration_{}",
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(8)
                .collect::<String>()
        );
        let authority = ZookeeperAuthority::new(&zk).unwrap();
        tokio::runtime::current_thread::block_on_all(
            WorkerBuilder::default()
                .start(Arc::new(authority))
                .and_then(|mut c| c.install_recipe(r_txt)),
        )
        .unwrap();
    }

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    fn it_works_default_local() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        {
            let _c = WorkerBuilder::default().start_simple().unwrap();
            thread::sleep(Duration::from_millis(100));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration_local() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = WorkerBuilder::default().start_simple().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
    }
}
