use channel::poll::{PollEvent, PollingLoop, ProcessResult};
use channel::tcp::TcpSender;

use consensus::{Authority, Epoch, STATE_KEY};
use dataflow::checktable::service::CheckTableServer;
use dataflow::{DomainConfig, PersistenceParameters};

use controller::domain_handle::DomainHandle;
use controller::inner::ControllerInner;
use controller::recipe::Recipe;
use coordination::CoordinationMessage;

#[cfg(test)]
use std::boxed::FnBox;
use std::io::{self, ErrorKind};
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};
use std::time::{self, Duration, Instant};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::process;

use failure::{self, Error};
use futures::{self, Future, Stream};
use hyper::{self, Method, StatusCode};
use hyper::header::ContentType;
use hyper::server::{Http, NewService, Request, Response, Service};
use mio::net::TcpListener;
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
mod worker_inner;

pub use controller::builder::ControllerBuilder;
pub use controller::handle::ControllerHandle;
pub use controller::inner::RpcError;
pub use controller::migrate::Migration;
pub use controller::mutator::{Mutator, MutatorBuilder, MutatorError};
pub use controller::getter::{Getter, ReadQuery, ReadReply, RemoteGetter, RemoteGetterBuilder};
pub(crate) use controller::getter::LocalOrNot;
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

/// Describes a running controller instance. A serialized version of this struct is stored in
/// ZooKeeper so that clients can reach the currently active controller.
#[derive(Serialize, Deserialize)]
pub(crate) struct ControllerDescriptor {
    pub external_addr: SocketAddr,
    pub internal_addr: SocketAddr,
    pub checktable_addr: SocketAddr,
    pub nonce: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerConfig {
    pub sharding: Option<usize>,
    pub partial_enabled: bool,
    pub domain_config: DomainConfig,
    pub persistence: PersistenceParameters,
    pub heartbeat_every: Duration,
    pub healthcheck_every: Duration,
    pub local_workers: usize,
    pub nworkers: usize,
    pub nreaders: usize,
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
            nreaders: 1,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub config: ControllerConfig,
    pub epoch: Epoch,
    pub recipe: (), // TODO: store all relevant state here.
}

enum ControlEvent {
    ControllerMessage(CoordinationMessage),
    SoupletMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<String, StatusCode>>,
    ),
    WonLeaderElection(ControllerState),
    LostLeaderElection(ControllerState, ControllerDescriptor),
    Shutdown,
    Error(Error),
    #[cfg(test)]
    ManualMigration(Box<for<'a, 's> FnBox(&'a mut ::controller::migrate::Migration<'s>) + Send>),
}

/// Runs the soup instance.
pub struct Controller<A: Authority + 'static> {
    worker: Option<WorkerInner>,
    inner: Option<ControllerInner>,

    log: slog::Logger,

    listen_addr: IpAddr,
    internal: ServingThread,
    external: ServingThread,
    souplet: ServingThread,
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
        let souplet = Self::souplet_listen(event_tx.clone(), SocketAddr::new(listen_addr, 0));

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
                    inner: None,
                    worker: None,
                    log,
                    internal,
                    external,
                    checktable,
                    souplet,
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
        loop {
            let event = match self.worker {
                Some(ref mut worker) => match receiver.recv_timeout(worker.heartbeat()) {
                    Ok(event) => event,
                    Err(RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(_) => break,
                },
                None => match receiver.recv() {
                    Ok(event) => event,
                    Err(_) => break,
                },
            };
            match event {
                ControlEvent::SoupletMessage(msg) => if let Some(ref mut worker) = self.worker {
                    worker.coordination_message(msg)
                },
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
                    self.worker.take().map(|w| w.shutdown());
                    self.inner = Some(ControllerInner::new(
                        self.listen_addr,
                        self.checktable,
                        self.log.clone(),
                        state.clone(),
                    ));
                    self.worker = Some(
                        WorkerInner::new(
                            self.listen_addr,
                            self.checktable,
                            self.internal.addr,
                            self.souplet.addr,
                            &state,
                            self.log.clone(),
                        ).unwrap(),
                    );
                }
                ControlEvent::LostLeaderElection(state, descriptor) => {
                    assert!(self.inner.is_none());
                    self.worker.take().map(|w| w.shutdown());
                    if let Ok(worker) = WorkerInner::new(
                        self.listen_addr,
                        descriptor.checktable_addr,
                        descriptor.internal_addr,
                        self.souplet.addr,
                        &state,
                        self.log.clone(),
                    ) {
                        self.worker = Some(worker);
                    }
                }
                ControlEvent::Shutdown => break,
                ControlEvent::Error(e) => panic!("{}", e),
                #[cfg(test)]
                ControlEvent::ManualMigration(f) => if let Some(ref mut inner) = self.inner {
                    inner.migrate(move |m| f.call_box((m,)));
                },
            }
        }
        self.external.stop();
        self.internal.stop();
        self.souplet.stop();
        self.worker.map(|w| w.shutdown());
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
                        if event_tx.send(ControlEvent::ControllerMessage(msg)).is_err() {
                            return ProcessResult::StopPolling;
                        }
                    }
                    PollEvent::Timeout => {}
                }
                ProcessResult::KeepPolling
            });
        })
    }

    fn souplet_listen(event_tx: Sender<ControlEvent>, addr: SocketAddr) -> ServingThread {
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
                        if event_tx.send(ControlEvent::SoupletMessage(msg)).is_err() {
                            return ProcessResult::StopPolling;
                        }
                    }
                    PollEvent::Timeout => {}
                }
                ProcessResult::KeepPolling
            });
        })
    }

    fn campaign(
        event_tx: Sender<ControlEvent>,
        authority: Arc<A>,
        descriptor: ControllerDescriptor,
        config: ControllerConfig,
    ) -> JoinHandle<()> {
        let descriptor = serde_json::to_vec(&descriptor).unwrap();
        let campaign_inner = move |event_tx: Sender<ControlEvent>| -> Result<(), Error> {
            let lost_election = |payload: Vec<u8>| -> Result<(), Error> {
                let descriptor: ControllerDescriptor = serde_json::from_slice(&payload[..])?;
                let state: ControllerState =
                    serde_json::from_slice(&authority.try_read(STATE_KEY).unwrap().unwrap())
                        .unwrap();
                event_tx
                    .send(ControlEvent::LostLeaderElection(state, descriptor))
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
                let epoch = match authority.become_leader(descriptor.clone())? {
                    Some(epoch) => epoch,
                    None => continue,
                };
                let state = authority.read_modify_write(
                    STATE_KEY,
                    |state: Option<ControllerState>| match state {
                        None => Ok(ControllerState {
                            config: config.clone(),
                            epoch,
                            recipe: (),
                        }),
                        Some(ref state) if state.epoch > epoch => Err(()),
                        Some(mut state) => {
                            state.epoch = epoch;
                            Ok(state)
                        }
                    },
                )?;
                if state.is_err() {
                    continue;
                }

                event_tx
                    .send(ControlEvent::WonLeaderElection(state.unwrap()))
                    .map_err(|_| failure::err_msg("ControlEvent send failed"))?;

                // LEADER STATE - manage system
                //
                // It is not currently possible to safely handle loss of leadership status (and
                // there is nothing that can currently trigger it), so we simply abort if the
                // current epoch ends.
                let _ = authority.await_new_epoch(epoch)?;
                eprintln!("Lost leadership?! Aborting...");
                process::abort();
            }
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
        assert!(c.install_recipe(r_txt.to_owned()).is_ok());
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
        assert!(c.install_recipe(r_txt.to_owned()).is_ok());
    }
}
