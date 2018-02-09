use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;

use mio::net::TcpListener;
use serde_json;
use slog::Logger;
use std::time::{Duration, Instant};

use channel::poll::{PollEvent, PollingLoop, ProcessResult};
use channel::{self, TcpSender};
use consensus::{Authority, Epoch, STATE_KEY};
use dataflow::prelude::*;
use dataflow::{DomainBuilder, Readers};

use controller::{ControllerDescriptor, ControllerState};
use coordination::{CoordinationMessage, CoordinationPayload};
use worker;

pub mod readers;

/// Souplets are responsible for running domains, and serving reads to any materializations
/// contained within them.
pub struct Souplet<A: Authority> {
    log: Logger,

    pool: Option<worker::WorkerPool>,

    // Controller connection
    authority: A,
    listen_addr: SocketAddr,
    epoch: Option<Epoch>,

    // Read RPC handling
    read_listen_addr: SocketAddr,
    reader_exit: Option<Arc<()>>,

    receiver: Option<PollingLoop<CoordinationMessage>>,
    sender: Option<TcpSender<CoordinationMessage>>,
    channel_coordinator: Arc<ChannelCoordinator>,
    readers: Readers,
    nworkers: usize,

    // liveness
    heartbeat_every: Duration,
    last_heartbeat: Option<Instant>,
}

impl<A: Authority> Souplet<A> {
    /// Create a new worker.
    pub fn new(
        authority: A,
        listen_addr: IpAddr,
        heartbeat_every: Duration,
        nworkers: usize,
        nreaders: usize,
        log: Logger,
    ) -> Self {
        let readers = Arc::new(Mutex::new(HashMap::new()));

        let listener = TcpListener::bind(&SocketAddr::new(listen_addr, 0)).unwrap();
        let read_listen_addr = listener.local_addr().unwrap();
        let builder = thread::Builder::new().name("read-dispatcher".to_owned());
        let reader_exit = Arc::new(());
        {
            let readers = readers.clone();
            let reader_exit = reader_exit.clone();
            builder
                .spawn(move || readers::serve(listener, readers, nreaders, reader_exit))
                .unwrap();
        }
        info!(log, "Listening for reads on {}", read_listen_addr);

        Souplet {
            log,
            pool: None,

            authority,
            epoch: None,
            listen_addr: SocketAddr::new(listen_addr, 0),
            read_listen_addr,
            reader_exit: Some(reader_exit),

            receiver: None,
            sender: None,
            channel_coordinator: Arc::new(ChannelCoordinator::new()),
            readers,
            nworkers,

            heartbeat_every,
            last_heartbeat: None,
        }
    }

    /// Run the worker.
    pub fn run(&mut self) {
        loop {
            let leader = self.authority.get_leader().unwrap();
            self.epoch = Some(leader.0);
            let descriptor: ControllerDescriptor = serde_json::from_slice(&leader.1).unwrap();
            let state: ControllerState =
                serde_json::from_slice(&self.authority.try_read(STATE_KEY).unwrap().unwrap())
                    .unwrap();

            let prefix = format!("{}-log-", state.config.persistence.log_prefix);
            let log_files: Vec<String> = fs::read_dir(".")
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().ok().map(|t| t.is_file()).unwrap_or(false))
                .map(|e| e.path().to_string_lossy().into_owned())
                .filter(|path| path.starts_with(&prefix))
                .collect();

            match TcpSender::connect(&descriptor.internal_addr, None) {
                Ok(s) => {
                    self.sender = Some(s);
                    self.last_heartbeat = Some(Instant::now());

                    let local_addr = match self.receiver {
                        Some(ref r) => r.get_listener_addr().unwrap(),
                        None => {
                            let listener = TcpListener::bind(&self.listen_addr).unwrap();
                            let addr = listener.local_addr().unwrap();
                            self.receiver = Some(PollingLoop::from_listener(listener));
                            addr
                        }
                    };

                    // say hello
                    let msg = self.wrap_payload(CoordinationPayload::Register {
                        addr: local_addr,
                        read_listen_addr: self.read_listen_addr,
                        log_files,
                    });

                    match self.sender.as_mut().unwrap().send(msg) {
                        Ok(_) => {
                            self.handle(descriptor.checktable_addr);
                            break;
                        }
                        Err(e) => error!(self.log, "failed to register with controller: {:?}", e),
                    }
                }
                Err(e) => error!(self.log, "failed to connect to controller: {:?}", e),
            }

            // wait for a second in between connection attempts
            thread::sleep(Duration::from_millis(1000));
        }
    }

    /// Main worker loop: waits for instructions from controller, and occasionally heartbeats to
    /// tell the controller that we're still here
    fn handle(&mut self, checktable_addr: SocketAddr) {
        // now that we're connected to a leader, we can start the pool
        self.pool = Some(
            worker::WorkerPool::new(
                self.nworkers,
                &self.log,
                checktable_addr,
                self.channel_coordinator.clone(),
            ).unwrap(),
        );

        // needed to make the borrow checker happy, replaced later
        let mut receiver = self.receiver.take();

        receiver.as_mut().unwrap().run_polling_loop(|e| {
            match e {
                PollEvent::ResumePolling(timeout) => {
                    *timeout = Some(self.heartbeat_every);
                    return ProcessResult::KeepPolling;
                }
                PollEvent::Process(msg) => {
                    trace!(self.log, "Received {:?}", msg);
                    match msg.payload {
                        CoordinationPayload::AssignDomain(d) => {
                            self.handle_domain_assign(d).unwrap()
                        }
                        CoordinationPayload::DomainBooted(domain, addr) => {
                            self.handle_domain_booted(domain, addr).unwrap()
                        }
                        _ => (),
                    }
                }
                PollEvent::Timeout => (),
            }

            match self.heartbeat() {
                Ok(_) => ProcessResult::KeepPolling,
                Err(e) => {
                    error!(self.log, "failed to send heartbeat to controller: {:?}", e);
                    ProcessResult::StopPolling
                }
            }
        });

        self.receiver = receiver;
    }

    fn handle_domain_assign(&mut self, d: DomainBuilder) -> Result<(), channel::tcp::SendError> {
        let listener = ::std::net::TcpListener::bind(self.listen_addr).unwrap();
        let addr = listener.local_addr().unwrap();

        let idx = d.index;
        let shard = d.shard;
        let d = d.build(
            self.log.clone(),
            self.readers.clone(),
            self.channel_coordinator.clone(),
            addr,
        );

        let listener = ::mio::net::TcpListener::from_listener(listener, &addr).unwrap();
        self.pool.as_mut().unwrap().add_replica(worker::NewReplica {
            inner: d,
            listener: listener,
        });

        // need to register the domain with the local channel coordinator
        self.channel_coordinator
            .insert_addr((idx, shard), addr, false);

        let msg = self.wrap_payload(CoordinationPayload::DomainBooted((idx, shard), addr));
        match self.sender.as_mut().unwrap().send(msg) {
            Ok(_) => {
                trace!(
                    self.log,
                    "informed controller that domain {}.{} is at {:?}",
                    idx.index(),
                    shard,
                    addr
                );
                Ok(())
            }
            Err(e) => return Err(e),
        }
    }

    fn handle_domain_booted(
        &mut self,
        (domain, shard): (DomainIndex, usize),
        addr: SocketAddr,
    ) -> Result<(), String> {
        trace!(
            self.log,
            "found that domain {}.{} is at {:?}",
            domain.index(),
            shard,
            addr
        );
        self.channel_coordinator
            .insert_addr((domain, shard), addr, false);
        Ok(())
    }

    fn wrap_payload(&self, pl: CoordinationPayload) -> CoordinationMessage {
        let addr = match self.sender {
            None => panic!("socket not connected, failed to send"),
            Some(ref s) => s.local_addr().unwrap(),
        };
        CoordinationMessage {
            source: addr,
            epoch: self.epoch.unwrap(),
            payload: pl,
        }
    }

    fn heartbeat(&mut self) -> Result<(), channel::tcp::SendError> {
        if self.last_heartbeat.is_some()
            && self.last_heartbeat.as_ref().unwrap().elapsed() > self.heartbeat_every
        {
            let msg = self.wrap_payload(CoordinationPayload::Heartbeat);
            match self.sender.as_mut().unwrap().send(msg) {
                Ok(_) => debug!(self.log, "sent heartbeat to controller"),
                Err(e) => return Err(e),
            }

            self.last_heartbeat = Some(Instant::now());
        }
        Ok(())
    }
}

impl<A: Authority> Drop for Souplet<A> {
    fn drop(&mut self) {
        drop(self.reader_exit.take());
        self.pool.as_mut().map(|p| p.wait());
    }
}
