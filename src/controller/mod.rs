use api::ControllerDescriptor;
use async_bincode::{AsyncBincodeReader, AsyncBincodeWriter};
use basics::DomainIndex;
use channel::{self, DualTcpReceiver, TcpSender, CONNECTION_FROM_BASE};
use consensus::{Authority, Epoch, STATE_KEY};
use controller::domain_handle::DomainHandle;
use controller::inner::ControllerInner;
use controller::recipe::Recipe;
use controller::sql::reuse::ReuseConfigType;
use coordination::{CoordinationMessage, CoordinationPayload};
use dataflow::{DomainBuilder, DomainConfig, Packet, PersistenceParameters, Readers};
use failure;
use futures::sync::mpsc::UnboundedSender;
use futures::{self, Future, Sink, Stream};
use hyper::header::CONTENT_TYPE;
use hyper::server;
use hyper::{self, Method, StatusCode};
use rand;
use serde_json;
use slog;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufReader, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    atomic::{AtomicUsize, Ordering}, Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{self, Duration};
use tokio;
use tokio::prelude::future::{poll_fn, Either};
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
type EnqueuedSend = (ReplicaIndex, Box<Packet>);
type ChannelCoordinator = channel::ChannelCoordinator<ReplicaIndex>;

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

/// Start up a new instance and return a handle to it. Dropping the handle will stop the
/// controller.
fn start_instance<A: Authority + 'static>(
    authority: Arc<A>,
    listen_addr: IpAddr,
    config: ControllerConfig,
    nworker_threads: usize,
    nread_threads: usize,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<Duration>,
    log: slog::Logger,
) -> Result<LocalControllerHandle<A>, failure::Error> {
    let rt = tokio::runtime::Runtime::new().unwrap();
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

    let mut instance = Instance {
        state: InstanceState::Pining,
        authority,
        controller: None,
        listen_addr,
        campaign,
        log,
    };

    rt.spawn(
        rx.map_err(|_| unreachable!())
            .for_each(move |e| {
                match e {
                    Event::InternalMessage(msg) => {
                        match msg.payload {
                            CoordinationPayload::Deregister => {
                                unimplemented!();
                            }
                            CoordinationPayload::RemoveDomain => {
                                unimplemented!();
                            }
                            CoordinationPayload::AssignDomain(d) => {
                                if let InstanceState::Active {
                                    epoch,
                                    ref mut add_domain,
                                } = instance.state
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
                                if let InstanceState::Active { epoch, .. } = instance.state {
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
                            CoordinationPayload::Register {
                                ref addr,
                                ref read_listen_addr,
                                ..
                            } => {
                                if let Some(ref mut ctrl) = instance.controller {
                                    // TODO: blocking
                                    ctrl.handle_register(&msg, addr, read_listen_addr.clone());
                                }
                            }
                            CoordinationPayload::Heartbeat => {
                                if let Some(ref mut ctrl) = instance.controller {
                                    ctrl.handle_heartbeat(&msg);
                                }
                            }
                        }
                    }
                    Event::ExternalRequest(method, path, body, reply_tx) => {
                        if let Some(ref mut ctrl) = instance.controller {
                            // TODO: blocking
                            let reply =
                                ctrl.external_request(method, path, body, &instance.authority);

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
                        if let Some(ref mut ctrl) = instance.controller {
                            if !ctrl.workers.is_empty() {
                                // TODO: blocking
                                ctrl.migrate(move |m| f.call_box((m,)));
                            }
                        }
                    }
                    Event::LeaderChange(state, descriptor) => {
                        if let InstanceState::Active { .. } = instance.state {
                            unimplemented!("leader change happened while running");
                        }

                        let (rep_tx, rep_rx) = futures::sync::mpsc::unbounded();
                        instance.state = InstanceState::Active {
                            epoch: state.epoch,
                            add_domain: rep_tx,
                        };

                        // now we can start accepting dataflow messages
                        // TODO: memory stuff should probably also be in config?
                        rt.spawn(
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
                    Event::WonLeaderElection(state) => {
                        // TODO: blocking
                        instance.campaign.take().unwrap().join().unwrap();
                        instance.controller = Some(ControllerInner::new(
                            instance.listen_addr,
                            instance.log.clone(),
                            state.clone(),
                        ));
                    }
                    Event::CampaignError(e) => {
                        panic!(e);
                    }
                    Event::Shutdown => {
                        /* TODO
                         * maybe use Receiver::close()?
                        self.external.stop();
                        self.internal.stop();
                        if self.controller.is_some() {
                            self.authority.surrender_leadership().unwrap();
                        }
                        rt.shutdown_now()
                        */
                        unimplemented!();
                    }
                }
                Either::B(futures::future::ok(()))
            })
            .map_err(|e| panic!(e)),
    );

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

pub struct Instance<A> {
    state: InstanceState,
    authority: Arc<A>,
    controller: Option<ControllerInner>,

    listen_addr: IpAddr,
    campaign: Option<JoinHandle<()>>,

    log: slog::Logger,
}

// I'm 95% sure this'll come back to bite me.
unsafe impl<A> Send for Instance<A> {}

fn listen_reads(
    on: tokio::net::TcpListener,
    readers: Readers,
) -> impl Future<Item = (), Error = ()> {
    on.incoming()
        .map(Some)
        .or_else(|e| {
            // io error from client: just ignore it
            Ok(None)
        })
        .filter_map(|c| c)
        .for_each(move |stream| {
            use tokio::prelude::AsyncRead;

            let mut readers = readers.clone();
            let (r, w) = stream.split();
            let mut w = AsyncBincodeWriter::from(w).for_sync();
            let mut r = AsyncBincodeReader::from(r);
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

    let mut sender = TcpSender::connect(&desc.internal_addr)?;
    let sender_addr = sender.local_addr()?;
    let (ctrl_tx, ctrl_rx) = futures::sync::mpsc::unbounded();

    // reader setup
    let readers = Arc::new(Mutex::new(HashMap::new()));
    let rport = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0))?;
    let raddr = rport.local_addr()?;

    // now async so we can use tokio::spawn
    Ok(futures::future::lazy(move || {
        // start controller message handler
        tokio::spawn(
            ctrl_rx
                .for_each(|cm| {
                    // TODO: deal with the case when the controller moves
                    // TODO: make these sends async
                    // TODO: blocking
                    sender.send(CoordinationMessage {
                        source: sender_addr,
                        payload: cm,
                        epoch,
                    });
                    Ok(())
                })
                .map_err(|e| panic!(e)),
        );

        // also start readers
        tokio::spawn(listen_reads(rport, readers.clone()));

        // and tell the controller about us
        ctrl_tx.send(CoordinationPayload::Register {
            addr: waddr,
            read_listen_addr: raddr,
            log_files,
        });

        let mut state_sizes = Arc::new(Mutex::new(HashMap::new()));
        if let Some(evict_every) = evict_every {
            let log = log.clone();
            let coord = coord.clone();
            let mut domain_senders = HashMap::new();
            tokio::spawn(
                tokio::timer::Interval::new(time::Instant::now() + evict_every, evict_every)
                    .for_each(move |_| {
                        poll_fn(|| {
                            blocking(|| {
                                do_eviction(
                                    &log,
                                    memory_limit,
                                    &coord,
                                    &mut domain_senders,
                                    &state_sizes,
                                );
                            })
                        }).map_err(|e| panic!(e))
                    })
                    .map_err(|e| panic!(e)),
            );
        }

        replicas
            .map_err(|e| panic!(e))
            .for_each(move |d| {
                let ok = do catch {
                    let on = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0))?;
                    let addr = on.local_addr()?;

                    let idx = d.index;
                    let shard = d.shard;
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
                    // TODO: self.state_sizes.insert((idx, shard), state_size);

                    let d = Arc::new(Mutex::new(d));
                    let ctrl = ctrl_tx.clone();
                    tokio::spawn(
                        tokio::timer::Interval::new(
                            time::Instant::now() + heartbeat_every,
                            heartbeat_every,
                        ).map(|_| CoordinationPayload::Heartbeat)
                            .map_err(|_| ())
                            .forward(ctrl.sink_map_err(|_| ()))
                            .map_err(|_| {
                                // we're probably just shutting down
                                ()
                            })
                            .map(|_| ()),
                    );

                    if let Some(evict_every) = evict_every {
                        let d = d.clone();
                        tokio::spawn(
                            tokio::timer::Interval::new(
                                time::Instant::now() + evict_every,
                                evict_every,
                            ).map_err(|e| panic!(e))
                                .for_each(move |_| {
                                    poll_fn(|| blocking(|| d.lock().unwrap().update_state_sizes()))
                                        .map_err(|e| panic!(e))
                                }),
                        );
                    }

                    ctrl_tx.send(CoordinationPayload::DomainBooted((idx, shard), addr));
                    trace!(
                        log,
                        "informed controller that domain {}.{} is at {:?}",
                        idx.index(),
                        shard,
                        addr
                    );

                    let log = log.clone();
                    on.incoming().for_each(move |stream| {
                        // we know that any new connection to a domain will first send a one-byte
                        // token to indicate whether the connection is from a base or not.
                        set_nonblocking(&stream, false);
                        let mut tag = [0];
                        use std::io::Read;
                        if let Err(e) = stream.read_exact(&mut tag[..]) {
                            // well.. that failed quickly..
                            info!(log, "worker discarded new connection: {:?}", e);
                            return Err(e);
                        }
                        let is_base = tag[0] == CONNECTION_FROM_BASE;
                        set_nonblocking(&stream, true);

                        debug!(log, "accepted new connection"; "base" => ?is_base);
                        let tcp = if is_base {
                            DualTcpReceiver::upgrade(BufReader::new(stream), move |input| {
                                Box::new(Packet::Input {
                                    inner: input,
                                    src: None, /* TODO: send_back */
                                    senders: Vec::new(),
                                })
                            })
                        } else {
                            BufReader::with_capacity(2 * 1024 * 1024, stream).into()
                        };

                        let d = Arc::clone(&d);

                        tokio::spawn(tcp.map_err(|e| panic!(e)).for_each(
                            move |dfm| -> Result<(), ()> {
                                // TODO: blocking
                                // TODO: worker::process
                                // TODO: carry_local
                                // TODO: capture any timeouts
                                // TODO: send_back
                                unimplemented!()
                            },
                        ));
                        Ok(())
                    })
                };

                match ok {
                    Ok(fut) => Either::A(fut),
                    Err(e) => Either::B(futures::future::err(e)),
                }
            })
            .map_err(|e| panic!(e))
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
            AsyncBincodeReader::from(sock)
                .map(Event::InternalMessage)
                .map_err(failure::Error::from)
                .forward(
                    event_tx
                        .clone()
                        .sink_map_err(|_| format_err!("main event loop went away")),
                )
                .map(|_| ())
        })
        .map_err(move |e| {
            warn!(log, "internal connection failed: {:?}", e);
        })
}

struct ExternalServer<A: Authority>(UnboundedSender<Event>, Arc<A>);
fn listen_external<A: Authority>(
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
            Box::new(match (req.method().clone(), req.uri().path()) {
                (Method::GET, "/graph.html") => {
                    res.header(CONTENT_TYPE, "text/html");
                    let res = res.body(hyper::Body::from(include_str!("graph.html")));
                    Either::A(futures::future::ok(res.unwrap()))
                }
                (Method::GET, "/js/layout-worker.js") => {
                    let res = res.body(hyper::Body::from(
                        "importScripts('https://cdn.rawgit.com/mstefaniuk/graph-viz-d3-js/\
                         cf2160ee3ca39b843b081d5231d5d51f1a901617/dist/layout-worker.js');",
                    ));
                    Either::A(futures::future::ok(res.unwrap()))
                }
                (Method::GET, path) if path.starts_with("/zookeeper/") => {
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
                    Either::A(futures::future::ok(res.unwrap()))
                }
                (method, path) => {
                    let path = path.to_owned();
                    let event_tx = self.0.clone();
                    Either::B(req.body().concat2().and_then(move |body| {
                        let body: Vec<u8> = body.iter().cloned().collect();
                        let (tx, rx) = futures::sync::oneshot::channel();
                        event_tx
                            .clone()
                            .send(Event::ExternalRequest(method, path, body, tx))
                            .map_err(|_| futures::Canceled)
                            .then(move |_| rx)
                            .and_then(|reply| {
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
                                futures::future::ok(res.unwrap())
                            })
                            .or_else(|_| {
                                res.header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                                res.status(StatusCode::NOT_FOUND);
                                futures::future::ok(res.body(hyper::Body::empty()).unwrap())
                            })
                    }))
                }
            })
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
    let campaign_inner = move |event_tx: UnboundedSender<Event>| -> Result<(), failure::Error> {
        let lost_election = |payload: Vec<u8>| -> Result<(), failure::Error> {
            let descriptor: ControllerDescriptor = serde_json::from_slice(&payload[..])?;
            let state: ControllerState =
                serde_json::from_slice(&authority.try_read(STATE_KEY).unwrap().unwrap()).unwrap();
            event_tx
                .send(Event::LeaderChange(state, descriptor))
                .wait()
                .map(|_| ())
                .map_err(|_| format_err!("send failed"))
        };

        loop {
            // WORKER STATE - watch for leadership changes
            //
            // If there is currently a leader, then loop until there is a period without a
            // leader, notifying the main thread every time a leader change occurs.
            let mut epoch;
            if let Some(leader) = authority.try_get_leader()? {
                epoch = leader.0;
                lost_election(leader.1)?;
                while let Some(leader) = authority.await_new_epoch(epoch)? {
                    epoch = leader.0;
                    lost_election(leader.1)?;
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
    coord: &Arc<ChannelCoordinator>,
    domain_senders: &mut HashMap<(DomainIndex, usize), TcpSender<Box<Packet>>>,
    state_sizes: &Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
) {
    use std::cmp;
    // 2. add current state sizes (could be out of date, as packet sent below is not
    //    necessarily received immediately)
    let sizes: Vec<(&(DomainIndex, usize), usize)> = state_sizes
        .lock()
        .unwrap()
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
            (ds, size)
        })
        .collect();
    let total: usize = sizes.iter().map(|&(_, s)| s).sum();
    // 3. are we above the limit?
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

                let tx = domain_senders.get_mut(largest.0).unwrap();
                tx.send(box Packet::Evict {
                    node: None,
                    num_bytes: cmp::min(largest.1, total - limit),
                }).unwrap();
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
        let mut c = ControllerBuilder::default().build(Arc::new(authority));
        assert!(c.install_recipe(r_txt).is_ok());
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
        assert!(c.install_recipe(r_txt).is_ok());
    }
}
