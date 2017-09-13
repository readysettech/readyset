use channel::{self, TcpSender};
use channel::poll::{PollEvent, PollingLoop, ProcessResult, RpcPollEvent, RpcPollingLoop};
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};

use distributary::{ChannelCoordinator, CoordinationMessage, CoordinationPayload, DomainBuilder,
                   NodeIndex, ReadQuery, ReadReply, SingleReadHandle};
use distributary::Index as DomainIndex;

pub struct Worker {
    log: Logger,

    // Controller connection
    controller_addr: String,
    listen_addr: String,
    listen_port: u16,

    // Read RPC handling
    read_listen_addr: SocketAddr,

    receiver: Option<PollingLoop<CoordinationMessage>>,
    sender: Option<TcpSender<CoordinationMessage>>,
    channel_coordinator: Arc<ChannelCoordinator>,
    domain_threads: Vec<JoinHandle<()>>,
    readers: Arc<Mutex<HashMap<(NodeIndex, usize), SingleReadHandle>>>,

    // liveness
    heartbeat_every: Duration,
    last_heartbeat: Option<Instant>,
}

impl Worker {
    fn serve_reads(
        mut polling_loop: RpcPollingLoop<ReadQuery, ReadReply>,
        readers: Arc<Mutex<HashMap<(NodeIndex, usize), SingleReadHandle>>>,
    ) {
        let mut readers_cache: HashMap<(NodeIndex, usize), SingleReadHandle> = HashMap::new();

        polling_loop.run_polling_loop(|event| match event {
            RpcPollEvent::ResumePolling(_) => ProcessResult::KeepPolling,
            RpcPollEvent::Timeout => unreachable!(),
            RpcPollEvent::Process(query, reply) => {
                *reply = Some(ReadReply(
                    query
                        .keys
                        .iter()
                        .map(|key| {
                            let reader = readers_cache.entry(query.target.clone()).or_insert_with(
                                || readers.lock().unwrap().get(&query.target).unwrap().clone(),
                            );

                            reader
                                .find_and(
                                    key,
                                    |rs| {
                                        rs.into_iter()
                                            .map(|r| r.iter().map(|v| v.deep_clone()).collect())
                                            .collect()
                                    },
                                    query.block,
                                )
                                .map(|r| r.0)
                                .map(|r| r.unwrap_or_else(Vec::new))
                        })
                        .collect(),
                ));
                ProcessResult::KeepPolling
            }
        });
        unreachable!();
    }

    pub fn new(
        controller: &str,
        listen_addr: &str,
        port: u16,
        heartbeat_every: Duration,
        log: Logger,
    ) -> Worker {
        use std::str::FromStr;

        let readers = Arc::new(Mutex::new(HashMap::new()));

        let readers_clone = readers.clone();
        let read_polling_loop = RpcPollingLoop::new(SocketAddr::from_str(listen_addr).unwrap());
        let read_listen_addr = read_polling_loop.get_listener_addr().unwrap();
        thread::spawn(move || Self::serve_reads(read_polling_loop, readers_clone));
        println!("Listening for reads on {:?}", read_listen_addr);

        Worker {
            log: log,

            listen_addr: String::from(listen_addr),
            listen_port: port,
            controller_addr: String::from(controller),

            read_listen_addr,

            receiver: None,
            sender: None,
            channel_coordinator: Arc::new(ChannelCoordinator::new()),
            domain_threads: Vec::new(),
            readers,

            heartbeat_every: heartbeat_every,
            last_heartbeat: None,
        }
    }

    /// Connect to controller
    pub fn connect(&mut self) -> Result<(), channel::tcp::SendError> {
        use mio::net::TcpListener;
        use std::str::FromStr;

        let local_addr = match self.receiver {
            Some(ref r) => r.get_listener_addr().unwrap(),
            None => {
                let listener = TcpListener::bind(&SocketAddr::from_str(
                    &format!("{}:{}", self.listen_addr, self.listen_port),
                ).unwrap())
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                self.receiver = Some(PollingLoop::from_listener(listener));
                addr
            }
        };

        let stream =
            TcpSender::connect(&SocketAddr::from_str(&self.controller_addr).unwrap(), None);
        match stream {
            Ok(s) => {
                self.sender = Some(s);
                self.last_heartbeat = Some(Instant::now());

                // say hello
                self.register(local_addr)?;

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Main worker loop: waits for instructions from controller, and occasionally heartbeats to
    /// tell the controller that we're still here
    pub fn handle(&mut self) {
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
        let idx = d.index;
        let shard = d.shard;
        let (jh, addr) = d.boot(
            self.log.clone(),
            self.readers.clone(),
            self.channel_coordinator.clone(),
        );
        self.domain_threads.push(jh);

        // need to register the domain with the local channel coordinator
        self.channel_coordinator.insert_addr((idx, shard), addr);

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
        self.channel_coordinator.insert_addr((domain, shard), addr);
        Ok(())
    }

    fn wrap_payload(&self, pl: CoordinationPayload) -> CoordinationMessage {
        let addr = match self.sender {
            None => panic!("socket not connected, failed to send"),
            Some(ref s) => s.local_addr().unwrap(),
        };
        CoordinationMessage {
            source: addr,
            payload: pl,
        }
    }

    fn heartbeat(&mut self) -> Result<(), channel::tcp::SendError> {
        if self.last_heartbeat.is_some() &&
            self.last_heartbeat.as_ref().unwrap().elapsed() > self.heartbeat_every
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

    fn register(&mut self, listen_addr: SocketAddr) -> Result<(), channel::tcp::SendError> {
        let msg = self.wrap_payload(CoordinationPayload::Register {
            addr: listen_addr,
            read_listen_addr: self.read_listen_addr,
        });
        self.sender.as_mut().unwrap().send(msg)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // wait for all domains to exit
        for t in self.domain_threads.drain(..) {
            t.join().unwrap();
        }
    }
}
