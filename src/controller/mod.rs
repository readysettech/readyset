use channel::poll::{PollEvent, PollingLoop, ProcessResult};
use channel::tcp::TcpSender;

use api::ControllerDescriptor;
use consensus::{Authority, Epoch, STATE_KEY};
use dataflow::{DomainConfig, PersistenceParameters};

use controller::domain_handle::DomainHandle;
use controller::inner::ControllerInner;
use controller::recipe::Recipe;
use controller::sql::reuse::ReuseConfigType;
use coordination::CoordinationMessage;

#[cfg(test)]
use std::boxed::FnBox;
use std::io::{self, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{self, Duration, Instant};

use failure::{self, Error};
use futures::{self, future::FutureResult, Future, IntoFuture, Stream};
use hyper::server::Server;
use hyper::service::{NewService, Service};
use hyper::{self, Method, Request, Response, StatusCode};
use mio::net::TcpListener;
use rand;
use serde_json;
use slog;
use tokio;

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
mod worker_inner;

pub use api::builders::*;
pub use api::prelude::*;
pub use controller::builder::ControllerBuilder;
pub use controller::getter::Getter;
pub use controller::handle::LocalControllerHandle;
pub use controller::migrate::Migration;
use controller::worker_inner::WorkerInner;

type WorkerIdentifier = SocketAddr;
type WorkerEndpoint = Arc<Mutex<TcpSender<CoordinationMessage>>>;

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

/// Wrapper around the state of a thread serving network requests. Both tracks the address that the
/// thread is listening on and provides a way to stop it.
struct ServingThread {
    addr: SocketAddr,
    join_handle: JoinHandle<()>,
    stop: Box<Drop + Send>,
}
impl ServingThread {
    fn new<F>(addr: SocketAddr, f: F) -> Self
    where
        F: FnOnce(TcpListener, Arc<()>) + Send + 'static,
    {
        let done = Arc::new(());
        let done2 = done.clone();
        let listener = TcpListener::bind(&addr).unwrap();
        let addr = listener.local_addr().unwrap();
        let builder = thread::Builder::new().name("souplet".to_owned());
        let join_handle = builder.spawn(move || f(listener, done)).unwrap();

        ServingThread {
            addr,
            join_handle,
            stop: Box::new(done2),
        }
    }
    fn stop(self) {
        drop(self.stop);
        self.join_handle.join().unwrap();
    }
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

enum ControlEvent {
    InternalMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Option<String>,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<Result<String, String>, StatusCode>>,
    ),
    WonLeaderElection(ControllerState),
    Shutdown,
    Error(Error),
    #[cfg(test)]
    ManualMigration(Box<for<'a, 's> FnBox(&'a mut ::controller::migrate::Migration<'s>) + Send>),
}
enum WorkerEvent {
    InternalMessage(CoordinationMessage),
    LeaderChange(ControllerState, ControllerDescriptor),
    Shutdown,
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
) -> LocalControllerHandle<A> {
    let (controller_event_tx, controller_event_rx) = mpsc::channel();
    let (worker_event_tx, worker_event_rx) = mpsc::channel();

    // Clone a bunch of items here before they get moved into one of the closures.
    let log2 = log.clone();
    let authority2 = authority.clone();
    let controller_event_tx2 = controller_event_tx.clone();
    let worker_event_tx2 = worker_event_tx.clone();
    let worker_event_tx3 = worker_event_tx.clone();

    let controller_join_handle = thread::Builder::new()
        .name("ctrl-main".to_owned())
        .spawn(move || {
            let internal = Controller::<A>::listen_internal(
                controller_event_tx.clone(),
                SocketAddr::new(listen_addr, 0),
            );
            let external = Controller::listen_external(
                controller_event_tx.clone(),
                SocketAddr::new(listen_addr, 0),
                authority.clone(),
            );
            let descriptor = ControllerDescriptor {
                external_addr: external.addr,
                internal_addr: internal.addr,
                nonce: rand::random(),
            };
            let campaign = Some(instance_campaign(
                controller_event_tx.clone(),
                worker_event_tx,
                authority.clone(),
                descriptor,
                config,
            ));

            let controller = Controller {
                authority,
                inner: None,
                receiver: controller_event_rx,
                log,
                internal,
                external,
                campaign,
                listen_addr,
            };
            controller.main_loop()
        })
        .unwrap();

    let worker_join_handle = thread::Builder::new()
        .name("worker-pool".to_owned())
        .spawn(move || {
            let internal =
                Worker::listen_internal(worker_event_tx2, SocketAddr::new(listen_addr, 0));
            let worker = Worker {
                inner: None,
                receiver: worker_event_rx,
                nworker_threads,
                nread_threads,
                memory_limit,
                memory_check_frequency,
                listen_addr,
                internal,
                log: log2,
            };
            worker.main_loop()
        })
        .unwrap();

    LocalControllerHandle::new(
        authority2,
        Some((controller_event_tx2, controller_join_handle)),
        Some((worker_event_tx3, worker_join_handle)),
    )
}
fn instance_campaign<A: Authority + 'static>(
    controller_event_tx: Sender<ControlEvent>,
    worker_event_tx: Sender<WorkerEvent>,
    authority: Arc<A>,
    descriptor: ControllerDescriptor,
    config: ControllerConfig,
) -> JoinHandle<()> {
    let descriptor_bytes = serde_json::to_vec(&descriptor).unwrap();
    let campaign_inner = move |controller_event_tx: Sender<ControlEvent>,
                               worker_event_tx: Sender<WorkerEvent>|
          -> Result<(), Error> {
        let lost_election = |payload: Vec<u8>| -> Result<(), Error> {
            let descriptor: ControllerDescriptor = serde_json::from_slice(&payload[..])?;
            let state: ControllerState =
                serde_json::from_slice(&authority.try_read(STATE_KEY).unwrap().unwrap()).unwrap();
            worker_event_tx
                .send(WorkerEvent::LeaderChange(state, descriptor))
                .map_err(|_| failure::err_msg("ControlEvent send failed"))?;
            Ok(())
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

            controller_event_tx
                .send(ControlEvent::WonLeaderElection(state.clone().unwrap()))
                .map_err(|_| failure::err_msg("ControlEvent send failed"))?;
            worker_event_tx
                .send(WorkerEvent::LeaderChange(
                    state.unwrap(),
                    descriptor.clone(),
                ))
                .map_err(|_| failure::err_msg("WorkerEvent send failed"))?;

            // LEADER STATE - manage system
            //
            // It is not currently possible to safely handle involuntary loss of leadership status
            // (and there is nothing that can currently trigger it), so don't bother watching for
            // it.
            return Ok(());
        }
    };

    thread::Builder::new()
        .name("srv-zk".to_owned())
        .spawn(move || {
            if let Err(e) = campaign_inner(controller_event_tx.clone(), worker_event_tx.clone()) {
                let _ = controller_event_tx.send(ControlEvent::Error(e));
                let _ = worker_event_tx.send(WorkerEvent::Shutdown);
            }
        })
        .unwrap()
}

/// Runs the soup instance.
pub struct Controller<A> {
    authority: Arc<A>,
    receiver: Receiver<ControlEvent>,
    inner: Option<ControllerInner>,

    listen_addr: IpAddr,
    internal: ServingThread,
    external: ServingThread,
    campaign: Option<JoinHandle<()>>,

    log: slog::Logger,
}
pub struct Worker {
    receiver: Receiver<WorkerEvent>,
    inner: Option<WorkerInner>,

    nworker_threads: usize,
    nread_threads: usize,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<Duration>,

    listen_addr: IpAddr,
    internal: ServingThread,

    log: slog::Logger,
}

impl<A: Authority + 'static> Controller<A> {
    fn main_loop(mut self) {
        for event in self.receiver {
            match event {
                ControlEvent::InternalMessage(msg) => if let Some(ref mut inner) = self.inner {
                    inner.coordination_message(msg)
                },
                ControlEvent::ExternalRequest(method, path, query, body, reply_tx) => {
                    if let Some(ref mut inner) = self.inner {
                        reply_tx
                            .send(inner.external_request(method, path, query, body, &self.authority))
                            .unwrap()
                    } else {
                        reply_tx.send(Err(StatusCode::NOT_FOUND)).unwrap();
                    }
                }
                ControlEvent::WonLeaderElection(state) => {
                    self.campaign.take().unwrap().join().unwrap();
                    self.inner = Some(ControllerInner::new(
                        self.listen_addr,
                        self.log.clone(),
                        state.clone(),
                    ));
                }
                ControlEvent::Shutdown => break,
                ControlEvent::Error(e) => panic!("{}", e),
                #[cfg(test)]
                ControlEvent::ManualMigration(f) => if let Some(ref mut inner) = self.inner {
                    if !inner.workers.is_empty() {
                        inner.migrate(move |m| f.call_box((m,)));
                    }
                },
            }
        }

        self.external.stop();
        self.internal.stop();

        if self.inner.is_some() {
            self.authority.surrender_leadership().unwrap();
        }
    }

    fn listen_external(
        event_tx: Sender<ControlEvent>,
        addr: SocketAddr,
        authority: Arc<A>,
    ) -> ServingThread
    where
        A: 'static,
    {
        struct ExternalServer<A: Authority>(Sender<ControlEvent>, Arc<A>);
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
                            res.header(hyper::header::CONTENT_TYPE, "text/html");
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
                                    res.header(hyper::header::CONTENT_TYPE, "application/json");
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
                        .send(ControlEvent::ExternalRequest(method, path, query, body, tx))
                        .map_err(|_| futures::Canceled)
                        .into_future()
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
            type Future = FutureResult<Self, Self::InitError>;
            fn new_service(&self) -> FutureResult<Self, Self::InitError> {
                Ok(self.clone()).into()
            }
        }

        let server = match Server::try_bind(&addr) {
            Ok(s) => s,
            Err(e) => {
                let s: Option<_> = do catch {
                    // https://github.com/hyperium/hyper/issues/1542
                    //let e = e.cause()?;
                    //let e = e.downcast_ref::<io::Error>()?;
                    //if e.kind() == ErrorKind::AddrInUse && addr.port() != 0 {
                    Some(Server::bind(&SocketAddr::new(addr.ip(), 0)))?
                    //} else {
                    //    None
                    //}?
                };

                if let Some(s) = s {
                    s
                } else {
                    panic!("{}", e);
                }
            }
        };

        let server = server.serve(ExternalServer(event_tx, authority));
        let addr = server.local_addr();

        let builder = thread::Builder::new().name("srv-ext".to_owned());
        let (done_tx, done_rx) = futures::sync::oneshot::channel::<()>();
        let join_handle = builder
            .spawn(move || {
                let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
                rt.spawn(server.map_err(|e| panic!("{}", e)));
                rt.block_on(done_rx.map_err(|_| ())).is_ok();
            })
            .unwrap();

        ServingThread {
            addr,
            join_handle,
            stop: Box::new(done_tx),
        }
    }

    /// Listen for messages from workers.
    fn listen_internal(event_tx: Sender<ControlEvent>, addr: SocketAddr) -> ServingThread {
        ServingThread::new(addr, move |listener, mut done| {
            let mut pl = PollingLoop::from_listener(listener);
            pl.run_polling_loop(move |e| {
                if Arc::get_mut(&mut done).is_some() {
                    return ProcessResult::StopPolling;
                }

                match e {
                    PollEvent::ResumePolling(timeout) => {
                        *timeout = Some(Duration::from_millis(100));
                    }
                    PollEvent::Process(msg) => {
                        if event_tx.send(ControlEvent::InternalMessage(msg)).is_err() {
                            return ProcessResult::StopPolling;
                        }
                    }
                    PollEvent::Timeout => {}
                }
                ProcessResult::KeepPolling
            });
        })
    }
}

impl Worker {
    fn main_loop(mut self) {
        loop {
            let event = match self.inner {
                Some(ref mut worker) => {
                    match self.receiver.recv_timeout(worker.do_housekeeping()) {
                        Ok(event) => event,
                        Err(RecvTimeoutError::Timeout) => {
                            continue;
                        }
                        Err(_) => break,
                    }
                }
                None => match self.receiver.recv() {
                    Ok(event) => event,
                    Err(_) => break,
                },
            };
            match event {
                WorkerEvent::InternalMessage(msg) => if let Some(ref mut worker) = self.inner {
                    worker.coordination_message(msg)
                },
                WorkerEvent::LeaderChange(state, descriptor) => {
                    info!(self.log, "Detected leader change");
                    self.inner.take().map(|w| w.shutdown());
                    info!(self.log, "Attempting to connect");
                    if let Ok(worker) = WorkerInner::new(
                        self.listen_addr,
                        descriptor.internal_addr,
                        self.internal.addr,
                        &state,
                        self.nworker_threads,
                        self.nread_threads,
                        self.memory_limit,
                        self.memory_check_frequency,
                        self.log.clone(),
                    ) {
                        self.inner = Some(worker);
                        warn!(self.log, "Connected to new leader");
                    } else {
                        warn!(self.log, "Failed to connect to new leader");
                    }
                }
                WorkerEvent::Shutdown => break,
            }
        }
        self.internal.stop();
        self.inner.map(|w| w.shutdown());
    }

    fn listen_internal(event_tx: Sender<WorkerEvent>, addr: SocketAddr) -> ServingThread {
        ServingThread::new(addr, move |listener, mut done| {
            let mut pl = PollingLoop::from_listener(listener);
            pl.run_polling_loop(move |e| {
                if Arc::get_mut(&mut done).is_some() {
                    return ProcessResult::StopPolling;
                }

                match e {
                    PollEvent::ResumePolling(timeout) => {
                        *timeout = Some(Duration::from_millis(100));
                    }
                    PollEvent::Process(msg) => {
                        if event_tx.send(WorkerEvent::InternalMessage(msg)).is_err() {
                            return ProcessResult::StopPolling;
                        }
                    }
                    PollEvent::Timeout => {}
                }
                ProcessResult::KeepPolling
            });
        })
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
