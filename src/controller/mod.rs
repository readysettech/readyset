use api::{ControllerDescriptor, Input};
use async_bincode::{AsyncBincodeReader, AsyncBincodeWriter, AsyncDestination, SyncDestination};
use basics::DomainIndex;
use bincode;
use bufstream::BufStream;
use channel::{
    self, poll::{PollEvent, ProcessResult}, DomainConnectionBuilder, DualTcpStream, TcpSender,
    CONNECTION_FROM_BASE,
};
use consensus::{Authority, Epoch, STATE_KEY};
use controller::domain_handle::DomainHandle;
use controller::inner::ControllerInner;
use controller::recipe::Recipe;
use controller::sql::reuse::ReuseConfigType;
use coordination::{CoordinationMessage, CoordinationPayload};
use dataflow::{
    payload::SourceChannelIdentifier, prelude::Executor, Domain, DomainBuilder, DomainConfig,
    Packet, PersistenceParameters, Readers,
};
use failure::{self, ResultExt};
use fnv::{FnvHashMap, FnvHashSet};
use futures::sync::mpsc::UnboundedSender;
use futures::{self, Future, Sink, Stream};
use hyper::header::CONTENT_TYPE;
use hyper::server;
use hyper::{self, Method, StatusCode};
use rand;
use serde_json;
use slog;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, BufWriter, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    atomic::{AtomicUsize, Ordering}, Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{self, Duration};
use streamunordered::StreamUnordered;
use tokio;
use tokio::prelude::future::{poll_fn, Either};
use tokio::prelude::*;
use tokio_threadpool::blocking;

#[cfg(test)]
use std::boxed::FnBox;

pub mod domain_handle;
pub mod keys;
pub mod migrate;

pub(crate) mod recipe;
pub(crate) mod security;
pub(crate) mod sql;

mod builder;
mod getter;
mod handle;
mod inner;
mod mir_to_flow;
mod mutator;
mod readers;

pub use api::builders::*;
pub use api::prelude::*;
pub use controller::builder::ControllerBuilder;
pub use controller::getter::Getter;
pub use controller::handle::LocalControllerHandle;
pub use controller::migrate::Migration;

type WorkerIdentifier = SocketAddr;
type WorkerEndpoint = Arc<Mutex<TcpSender<CoordinationMessage>>>;

type ReplicaIndex = (DomainIndex, usize);
type ChannelCoordinator = channel::ChannelCoordinator<ReplicaIndex>;

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
pub(crate) struct ControllerConfig {
    pub sharding: Option<usize>,
    pub partial_enabled: bool,
    pub domain_config: DomainConfig,
    pub persistence: PersistenceParameters,
    pub heartbeat_every: Duration,
    pub healthcheck_every: Duration,
    pub quorum: usize,
    pub reuse: ReuseConfigType,
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
                replay_batch_timeout: time::Duration::new(0, 10_000),
            },
            persistence: Default::default(),
            heartbeat_every: Duration::from_secs(1),
            healthcheck_every: Duration::from_secs(10),
            quorum: 1,
            reuse: ReuseConfigType::Finkelstein,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub config: ControllerConfig,
    pub epoch: Epoch,

    pub recipe_version: usize,
    pub recipes: Vec<String>,
}

enum Event {
    InternalMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<Result<String, String>, StatusCode>>,
    ),
    LeaderChange(ControllerState, ControllerDescriptor),
    WonLeaderElection(ControllerState),
    CampaignError(failure::Error),
    Shutdown,
    #[cfg(test)]
    ManualMigration(Box<for<'a, 's> FnBox(&'a mut ::controller::migrate::Migration<'s>) + Send>),
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
            Event::Shutdown => write!(f, "Shutdown"),
            #[cfg(test)]
            Event::ManualMigration(..) => write!(f, "ManualMigration(..)"),
        }
    }
}

/// Start up a new instance and return a handle to it. Dropping the handle will stop the
/// controller.
fn start_instance<A: Authority + 'static>(
    authority: Arc<A>,
    listen_addr: IpAddr,
    config: ControllerConfig,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<Duration>,
    log: slog::Logger,
) -> Result<LocalControllerHandle<A>, failure::Error> {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let (tx, rx) = futures::sync::mpsc::unbounded();

    // we'll be listening for a couple of different types of events:
    // first, events from workers
    let wport = tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0))?;
    let waddr = wport.local_addr()?;
    rt.spawn(listen_internal(log.clone(), tx.clone(), wport));

    // and second, messages from the "real world"
    let xport = match tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 9000)) {
        Ok(s) => Ok(s),
        Err(ref e) if e.kind() == ErrorKind::AddrInUse => {
            tokio::net::TcpListener::bind(&SocketAddr::new(listen_addr, 0))
        }
        Err(e) => Err(e),
    }?;
    let xaddr = xport.local_addr()?;
    let ext_log = log.clone();
    rt.spawn(
        listen_external(tx.clone(), xport.incoming(), authority.clone()).map_err(move |e| {
            warn!(ext_log, "external request failed: {:?}", e);
        }),
    );

    // shared df state
    let coord = Arc::new(ChannelCoordinator::new());

    // note that we do not start up the data-flow until we find a controller!

    let descriptor = ControllerDescriptor {
        external_addr: xaddr,
        internal_addr: waddr,
        nonce: rand::random(),
    };
    let campaign = Some(instance_campaign(
        tx.clone(),
        authority.clone(),
        descriptor,
        config,
    ));

    // set up different loops for the controller "part" and the worker "part" of us. this is
    // necessary because sometimes the two need to communicate (e.g., for migrations), and if they
    // were in a single loop, that could deadlock.
    let (ctrl_tx, ctrl_rx) = futures::sync::mpsc::unbounded();
    let (worker_tx, worker_rx) = futures::sync::mpsc::unbounded();

    // first, a loop that just forwards to the appropriate place
    rt.spawn(
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
                    },
                    Event::ExternalRequest(..) => fw(e, true),
                    #[cfg(test)]
                    Event::ManualMigration(..) => fw(e, true),
                    Event::LeaderChange(..) => fw(e, false),
                    Event::WonLeaderElection(..) => fw(e, true),
                    Event::CampaignError(..) => fw(e, true),
                    Event::Shutdown => {
                        // FIXME
                        // maybe use Receiver::close()?
                        // self.external.stop();
                        // self.internal.stop();
                        // if self.controller.is_some() {
                        //     self.authority.surrender_leadership().unwrap();
                        // }
                        // rt.shutdown_now()
                        unimplemented!();
                    }
                }.map_err(|e| panic!("{:?}", e))
            })
            .map(|_| ()),
    );

    {
        let mut worker_state = InstanceState::Pining;
        let log = log.clone();
        rt.spawn(
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
                            CoordinationPayload::DomainBooted((domain, shard), addr) => {
                                if let InstanceState::Active { epoch, .. } = worker_state {
                                    if epoch == msg.epoch {
                                        trace!(
                                            log,
                                            "found that domain {}.{} is at {:?}",
                                            domain.index(),
                                            shard,
                                            addr
                                        );
                                        coord.insert_addr((domain, shard), addr, false);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        },
                        Event::LeaderChange(state, descriptor) => {
                            if let InstanceState::Active { .. } = worker_state {
                                unimplemented!("leader change happened while running");
                            }

                            let (rep_tx, rep_rx) = futures::sync::mpsc::unbounded();
                            worker_state = InstanceState::Active {
                                epoch: state.epoch,
                                add_domain: rep_tx,
                            };

                            // now we can start accepting dataflow messages
                            // TODO: memory stuff should probably also be in config?
                            tokio::spawn(
                                listen_df(
                                    log.clone(),
                                    (memory_limit, memory_check_frequency),
                                    &state,
                                    &descriptor,
                                    waddr,
                                    coord.clone(),
                                    listen_addr,
                                    rep_rx,
                                ).unwrap(),
                            );
                        }
                        Event::Shutdown => {
                            unimplemented!();
                        }
                        e => unreachable!("{:?} is not a worker event", e),
                    }
                    Either::B(futures::future::ok(()))
                })
                .map_err(|e| panic!("{:?}", e)),
        );
    }

    {
        let log = log;
        let authority = authority.clone();
        let mut campaign = campaign;
        let mut controller: Option<ControllerInner> = None;
        rt.spawn(
            ctrl_rx
                .map_err(|_| unreachable!())
                .for_each(move |e| {
                    match e {
                        Event::InternalMessage(msg) => match msg.payload {
                            CoordinationPayload::Deregister => {
                                unimplemented!();
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
                        Event::ExternalRequest(method, path, body, reply_tx) => {
                            if let Some(ref mut ctrl) = controller {
                                let authority = &authority;
                                let reply = block_on(|| {
                                    ctrl.external_request(method, path, body, authority)
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
                        Event::ManualMigration(f) => {
                            if let Some(ref mut ctrl) = controller {
                                if !ctrl.workers.is_empty() {
                                    block_on(|| ctrl.migrate(move |m| f.call_box((m,))));
                                }
                            }
                        }
                        Event::WonLeaderElection(state) => {
                            let c = campaign.take().unwrap();
                            block_on(move || c.join().unwrap());
                            controller = Some(ControllerInner::new(
                                listen_addr,
                                log.clone(),
                                state.clone(),
                            ));
                        }
                        Event::CampaignError(e) => {
                            panic!("{:?}", e);
                        }
                        Event::Shutdown => {
                            unimplemented!();
                        }
                        e => unreachable!("{:?} is not a controller event", e),
                    }
                    Ok(())
                })
                .map_err(|e| panic!("{:?}", e)),
        );
    }

    Ok(LocalControllerHandle::new(authority, tx, rt))
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
        add_domain: UnboundedSender<DomainBuilder>,
    },
}

fn listen_reads(
    on: tokio::net::TcpListener,
    readers: Readers,
) -> impl Future<Item = (), Error = ()> {
    on.incoming()
        .map(Some)
        .or_else(|_| {
            // io error from client: just ignore it
            Ok(None)
        })
        .filter_map(|c| c)
        .for_each(move |stream| {
            use tokio::prelude::AsyncRead;

            let mut readers = readers.clone();
            let (r, w) = stream.split();
            let w = AsyncBincodeWriter::from(w);
            let r = AsyncBincodeReader::from(r);
            tokio::spawn(
                r.and_then(move |req| readers::handle_message(req, &mut readers))
                    .map_err(|_| -> () {
                        unreachable!();
                    })
                    .forward(w.sink_map_err(|_| ()))
                    .then(|_| {
                        // we're probably just shutting down
                        Ok(())
                    }),
            );
            Ok(())
        })
}

fn listen_df(
    log: slog::Logger,
    (memory_limit, evict_every): (Option<usize>, Option<Duration>),
    state: &ControllerState,
    desc: &ControllerDescriptor,
    waddr: SocketAddr,
    coord: Arc<ChannelCoordinator>,
    on: IpAddr,
    replicas: futures::sync::mpsc::UnboundedReceiver<DomainBuilder>,
) -> Result<impl Future<Item = (), Error = ()>, failure::Error> {
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

    // now async so we can use tokio::spawn
    Ok(tokio::net::TcpStream::connect(&desc.internal_addr)
        .map_err(|e| panic!("{:?}", e))
        .and_then(move |sender| {
            let sender_addr = sender.local_addr().unwrap();
            let sender = AsyncBincodeWriter::from(sender).for_async();

            // start controller message handler
            // TODO: deal with the case when the controller moves
            tokio::spawn(
                ctrl_rx
                    .map(move |cm| CoordinationMessage {
                        source: sender_addr,
                        payload: cm,
                        epoch,
                    })
                    .map_err(|e| panic!("{:?}", e))
                    .forward(sender.sink_map_err(|e| panic!("{:?}", e)))
                    .map(|_| ()),
            );

            // also start readers
            tokio::spawn(listen_reads(rport, readers.clone()));

            // and tell the controller about us
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
                        tokio::timer::Interval::new(
                            time::Instant::now() + heartbeat_every,
                            heartbeat_every,
                        ).map(|_| CoordinationPayload::Heartbeat)
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
                tokio::spawn(
                    tokio::timer::Interval::new(time::Instant::now() + evict_every, evict_every)
                        .for_each(move |_| {
                            do_eviction(&log, memory_limit, &mut domain_senders, &state_sizes)
                                .map_err(|e| panic!("{:?}", e))
                        })
                        .map_err(|e| panic!("{:?}", e)),
                );
            }

            replicas
                .map_err(|e| -> io::Error { panic!("{:?}", e) })
                .fold(ctrl_tx, move |ctrl_tx, d| {
                    let idx = d.index;
                    let shard = d.shard;
                    let addr: io::Result<_> = do catch {
                        let on = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0))?;
                        let addr = on.local_addr()?;

                        let state_size = Arc::new(AtomicUsize::new(0));
                        let d = d.build(
                            log.clone(),
                            readers.clone(),
                            coord.clone(),
                            addr,
                            state_size.clone(),
                        );

                        // need to register the domain with the local channel coordinator
                        coord.insert_addr((idx, shard), addr, false);
                        block_on(|| state_sizes.lock().unwrap().insert((idx, shard), state_size));

                        tokio::spawn(Replica::new(d, on, log.clone(), coord.clone()));

                        trace!(
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
                                .clone()
                                .send(CoordinationPayload::DomainBooted((idx, shard), addr))
                                .map_err(|_| {
                                    // controller went away -- exit?
                                    io::Error::new(io::ErrorKind::Other, "controller went away")
                                }),
                        ),
                        Err(e) => Either::B(future::err(e)),
                    }
                })
                .map_err(|e| panic!("{:?}", e))
                .map(|_| ())
        }))
}

fn listen_internal(
    log: slog::Logger,
    event_tx: UnboundedSender<Event>,
    on: tokio::net::TcpListener,
) -> impl Future<Item = (), Error = ()> {
    on.incoming()
        .map_err(failure::Error::from)
        .for_each(move |sock| {
            tokio::spawn(
                AsyncBincodeReader::from(sock)
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
    on: tokio::net::Incoming,
    authority: Arc<A>,
) -> impl Future<Item = (), Error = hyper::Error> + Send {
    use hyper::{
        service::{NewService, Service}, Request, Response,
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
            let event_tx = self.0.clone();
            Box::new(req.into_body().concat2().and_then(move |body| {
                let body: Vec<u8> = body.iter().cloned().collect();
                let (tx, rx) = futures::sync::oneshot::channel();
                event_tx
                    .clone()
                    .send(Event::ExternalRequest(method, path, body, tx))
                    .map_err(|_| futures::Canceled)
                    .then(move |_| rx)
                    .then(move |reply| match reply {
                        Ok(reply) => {
                            res.header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                            let res = match reply {
                                Ok(Ok(reply)) => res.body(hyper::Body::from(reply)),
                                Ok(Err(reply)) => {
                                    res.status(StatusCode::INTERNAL_SERVER_ERROR);
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
                            res.header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
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
    server::Server::builder(on).serve(service)
}

// NOTE: tokio::net::TcpStream doesn't expose underlying stream :(
fn set_nonblocking(s: &tokio::net::TcpStream, on: bool) {
    use std::net::TcpStream;
    use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
    let t = unsafe { TcpStream::from_raw_fd(s.as_raw_fd()) };
    t.set_nonblocking(on).unwrap();
    // avoid closing on Drop
    t.into_raw_fd();
}

fn instance_campaign<A: Authority + 'static>(
    event_tx: UnboundedSender<Event>,
    authority: Arc<A>,
    descriptor: ControllerDescriptor,
    config: ControllerConfig,
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
                    }).unwrap()
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

    incoming: tokio::net::Incoming,
    inputs: StreamUnordered<
        DualTcpStream<BufStream<tokio::net::TcpStream>, Box<Packet>, Input, SyncDestination>,
    >,
    outputs: FnvHashMap<
        ReplicaIndex,
        (
            AsyncBincodeWriter<BufWriter<tokio::net::TcpStream>, Box<Packet>, AsyncDestination>,
            bool,
            bool,
        ),
    >,

    outbox: FnvHashMap<ReplicaIndex, VecDeque<Box<Packet>>>,
    timeout: Option<tokio::timer::Delay>,
    sendback: Sendback,
}

impl Replica {
    pub fn new(
        domain: Domain,
        on: tokio::net::TcpListener,
        log: slog::Logger,
        cc: Arc<ChannelCoordinator>,
    ) -> Self {
        Replica {
            coord: cc,
            domain,
            incoming: on.incoming(),
            log,
            inputs: Default::default(),
            outputs: Default::default(),
            outbox: Default::default(),
            sendback: Default::default(),
            timeout: None,
        }
    }

    fn try_ack(&mut self) -> Result<(), failure::Error> {
        let inputs = &mut self.inputs;
        let pending = &mut self.sendback.pending;

        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        self.sendback.back.retain(|&streami, n| {
            use std::ops::SubAssign;

            let stream = &mut inputs[streami];

            let end = *n;
            for i in 0..end {
                match stream.start_send(()) {
                    Ok(AsyncSink::Ready) => {
                        if i == 0 {
                            pending.insert(streami);
                        }
                        n.sub_assign(1);
                    }
                    Ok(AsyncSink::NotReady(())) => {
                        break;
                    }
                    Err(e) => {
                        // start_send shouldn't generally error
                        err.push(e.into());
                    }
                }
            }

            *n > 0
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

        // just like in try_ack:
        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        for (&ri, ms) in &mut self.outbox {
            if ms.is_empty() {
                continue;
            }

            let &mut (ref mut tx, ref mut pending, is_local) =
                outputs.entry(ri).or_insert_with(|| {
                    let mut dest = None;
                    while dest.is_none() {
                        dest = cc.get_dest(&ri);
                    }
                    let (addr, is_local) = dest.unwrap();
                    let tx = DomainConnectionBuilder::for_domain(addr)
                        .build_async()
                        .unwrap();
                    (tx, true, is_local)
                });

            while let Some(mut m) = ms.pop_front() {
                if is_local && !m.is_local() {
                    m = m.make_local();
                }

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
        for &mut (ref mut tx, ref mut pending, _) in outputs.values_mut() {
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

    fn try_new(&mut self) -> io::Result<()> {
        while let Async::Ready(Some(mut stream)) = self.incoming.poll()? {
            // we know that any new connection to a domain will first send a one-byte
            // token to indicate whether the connection is from a base or not.
            set_nonblocking(&stream, false);
            let mut tag = [0];
            use std::io::Read;
            if let Err(e) = stream.read_exact(&mut tag[..]) {
                // well.. that failed quickly..
                info!(self.log, "worker discarded new connection: {:?}", e);
            } else {
                let is_base = tag[0] == CONNECTION_FROM_BASE;
                set_nonblocking(&stream, true);

                debug!(self.log, "accepted new connection"; "base" => ?is_base);
                let slot = self.inputs.stream_slot();
                let token = slot.token();
                let tcp = if is_base {
                    DualTcpStream::upgrade(BufStream::new(stream), move |input| {
                        Box::new(Packet::Input {
                            inner: input,
                            src: Some(SourceChannelIdentifier { token }),
                            senders: Vec::new(),
                        })
                    })
                } else {
                    BufStream::with_capacities(2 * 1024 * 1024, 4 * 1024, stream).into()
                };
                slot.insert(tcp);
            }
        }
        Ok(())
    }

    fn try_timeout(&mut self) -> Result<(), tokio::timer::Error> {
        if let Some(mut to) = self.timeout.take() {
            if let Async::Ready(()) = to.poll()? {
                block_on(|| {
                    self.domain
                        .on_event(&mut self.sendback, PollEvent::Timeout, &mut self.outbox)
                });
            } else {
                self.timeout = Some(to);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Sendback {
    // map from inputi to number of (empty) ACKs
    back: FnvHashMap<usize, usize>,
    pending: FnvHashSet<usize>,
}

impl Executor for Sendback {
    fn send_back(&mut self, id: SourceChannelIdentifier, _: ()) {
        use std::ops::AddAssign;
        self.back.entry(id.token).or_insert(0).add_assign(1);
    }
}

impl Future for Replica {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let r: Result<Async<Self::Item>, failure::Error> = do catch {
            // FIXME: check if we should call update_state_sizes (every evict_every)

            // first, try sending packets to downstream domains if they blocked before
            // TODO: send fail == exiting?
            self.try_flush().context("downstream flush (before)")?;

            // then, try to send any acks we haven't sent yet
            self.try_ack()?;

            // then, see if there are any new connections
            self.try_new().context("check for new connections")?;

            // then, see if our timer has expired
            self.try_timeout().context("check timeout")?;

            // and now, finally, we see if there's new input for us
            loop {
                match self.inputs.poll() {
                    Ok(Async::Ready(Some((packet, _)))) => {
                        let d = &mut self.domain;
                        let sb = &mut self.sendback;
                        let ob = &mut self.outbox;

                        // TODO: carry_local
                        if let ProcessResult::StopPolling =
                            block_on(|| d.on_event(sb, PollEvent::Process(packet), ob))
                        {
                            // TODO: quit
                            unimplemented!();
                        }
                    }
                    Ok(Async::Ready(None)) => {
                        warn!(self.log, "all streams terminated");
                        // XXX: time to exit?
                        break;
                    }
                    Ok(Async::NotReady) => break,
                    Err(e) => {
                        error!(self.log, "input stream failed: {:?}", e);
                        break;
                    }
                }
            }

            // check if we now need to set a timeout
            let mut timeout = None;
            self.domain.on_event(
                &mut self.sendback,
                PollEvent::ResumePolling(&mut timeout),
                &mut self.outbox,
            );

            if let Some(timeout) = timeout {
                self.timeout = Some(tokio::timer::Delay::new(time::Instant::now() + timeout));

                // we need to poll the delay to ensure we'll get woken up
                self.try_timeout().context("check timeout after setting")?;
            }

            // send to downstream
            // TODO: send fail == exiting?
            self.try_flush().context("downstream flush (after)")?;

            // and also acks
            self.try_ack()?;

            Async::NotReady
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
    use consensus::ZookeeperAuthority;

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    #[allow_fail]
    fn it_works_default() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_default").unwrap();
        {
            let _c = ControllerBuilder::default().build(Arc::new(authority));
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

        let authority =
            ZookeeperAuthority::new("127.0.0.1:2181/it_works_blender_with_migration").unwrap();
        let mut c = ControllerBuilder::default()
            .build(Arc::new(authority))
            .unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
    }

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    fn it_works_default_local() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        {
            let _c = ControllerBuilder::default().build_local().unwrap();
            thread::sleep(Duration::from_millis(100));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration_local() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = ControllerBuilder::default().build_local().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
    }
}
