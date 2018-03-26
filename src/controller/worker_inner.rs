use channel;
use channel::tcp::{TcpSender, TryRecvError};
use channel::rpc::RpcServiceEndpoint;
use consensus::Epoch;
use dataflow::{DomainBuilder, Readers};
use dataflow::payload;
use dataflow::prelude::{ChannelCoordinator, DomainIndex};

use controller::{readers, ControllerState};
use coordination::{CoordinationMessage, CoordinationPayload};
use worker;

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs;

use mio::net::TcpListener;
use mio_pool::{PoolBuilder, PoolHandle};
use slog;

pub(super) struct WorkerInner {
    epoch: Epoch,
    worker_pool: worker::WorkerPool,
    channel_coordinator: Arc<ChannelCoordinator>,
    readers: Readers,
    read_threads: PoolHandle<()>,

    sender: TcpSender<CoordinationMessage>,
    sender_addr: SocketAddr,

    heartbeat_every: Duration,
    last_heartbeat: Instant,

    listen_addr: IpAddr,

    memory_limit: usize,
    state_sizes: HashMap<(DomainIndex, usize), Arc<AtomicUsize>>,

    log: slog::Logger,
}

impl WorkerInner {
    pub(super) fn new(
        listen_addr: IpAddr,
        checktable_addr: SocketAddr,
        controller_addr: SocketAddr,
        souplet_addr: SocketAddr,
        state: &ControllerState,
        nworker_threads: usize,
        nread_threads: usize,
        memory_limit: usize,
        log: slog::Logger,
    ) -> Result<WorkerInner, ()> {
        let channel_coordinator = Arc::new(ChannelCoordinator::new());
        let readers = Arc::new(Mutex::new(HashMap::new()));

        let log_prefix = state.config.persistence.log_prefix.clone();
        let prefix = format!("{}-log-", log_prefix);
        let log_files: Vec<String> = fs::read_dir(".")
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().ok().map(|t| t.is_file()).unwrap_or(false))
            .map(|e| e.path().to_string_lossy().into_owned())
            .filter(|path| path.starts_with(&prefix))
            .collect();

        let (read_threads, read_listen_addr) = Self::reads_listen(
            SocketAddr::new(listen_addr, 0),
            readers.clone(),
            nread_threads,
        );

        let mut sender = match TcpSender::connect(&controller_addr) {
            Ok(sender) => sender,
            Err(e) => {
                error!(log, "failed to connect to controller: {:?}", e);
                return Err(());
            }
        };

        let sender_addr = sender.local_addr().unwrap();
        let msg = CoordinationMessage {
            source: sender_addr,
            epoch: state.epoch,
            payload: CoordinationPayload::Register {
                addr: souplet_addr,
                read_listen_addr,
                log_files,
            },
        };

        if let Err(e) = sender.send(msg) {
            error!(log, "failed to register with controller: {:?}", e);
            return Err(());
        }

        Ok(WorkerInner {
            epoch: state.epoch,
            worker_pool: worker::WorkerPool::new(
                nworker_threads,
                &log,
                checktable_addr,
                channel_coordinator.clone(),
            ).unwrap(),
            channel_coordinator,
            read_threads,
            readers,
            sender,
            sender_addr,
            heartbeat_every: state.config.heartbeat_every,
            last_heartbeat: Instant::now(),
            listen_addr,
            memory_limit,
            state_sizes: HashMap::new(),
            log,
        })
    }

    pub(super) fn coordination_message(&mut self, msg: CoordinationMessage) {
        if self.epoch == msg.epoch {
            match msg.payload {
                CoordinationPayload::AssignDomain(d) => self.handle_domain_assign(d).unwrap(),
                CoordinationPayload::DomainBooted(domain, addr) => {
                    self.handle_domain_booted(domain, addr).unwrap()
                }
                _ => unreachable!(),
            }
        }
    }

    fn handle_domain_assign(&mut self, d: DomainBuilder) -> Result<(), channel::tcp::SendError> {
        let listener = ::std::net::TcpListener::bind(SocketAddr::new(self.listen_addr, 0)).unwrap();
        let addr = listener.local_addr().unwrap();

        let idx = d.index;
        let shard = d.shard;
        let state_size = Arc::new(AtomicUsize::new(0));
        let d = d.build(
            self.log.clone(),
            self.readers.clone(),
            self.channel_coordinator.clone(),
            addr,
            state_size.clone(),
        );

        let listener = ::mio::net::TcpListener::from_std(listener).unwrap();
        self.worker_pool
            .add_replica(worker::NewReplica { inner: d, listener });

        // need to register the domain with the local channel coordinator
        self.channel_coordinator
            .insert_addr((idx, shard), addr, false);
        self.state_sizes.insert((idx, shard), state_size);

        let msg = CoordinationMessage {
            source: self.sender_addr,
            epoch: self.epoch,
            payload: CoordinationPayload::DomainBooted((idx, shard), addr),
        };

        match self.sender.send(msg) {
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

    /// Perform a heartbeat if it is time, and return the amount of time until another one is
    /// needed.
    pub(super) fn heartbeat(&mut self) -> Duration {
        let elapsed = self.last_heartbeat.elapsed();
        if elapsed > self.heartbeat_every {
            // also check own state size
            // 1. tell domains to update state size
            for &(di, shard) in self.state_sizes.keys() {
                let mut tx = self.channel_coordinator.get_tx(&(di, shard));
                // we're lax about failures here since missing an UpdateStateSize message has
                // no correctness implications
                match tx {
                    // domain may already have exited
                    None => continue,
                    Some(mut tx) => match tx.0.send(box payload::Packet::UpdateStateSize) {
                        Ok(_) => (),
                        Err(_) => error!(self.log, "failed to send UpdateStateSize to domain"),
                    },
                }
            }
            // 2. add current state sizes (could be out of date, as packet sent below is not
            //    necessarily received immediately)
            let sizes: Vec<(&(DomainIndex, usize), usize)> = self.state_sizes
                .iter()
                .map(|(ds, sa)| {
                    let size = sa.load(Ordering::Relaxed);
                    debug!(
                        self.log,
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
            if total >= self.memory_limit {
                error!(
                    self.log,
                    "aggregate domain state ({} bytes) exceeds memory limit ({} bytes)",
                    total,
                    self.memory_limit
                );
                // evict from the largest domain
                let largest = sizes.into_iter().max_by_key(|&(_, s)| s).unwrap();
                warn!(self.log, "evicting from {:?}", largest);
                let mut tx = self.channel_coordinator.get_tx(largest.0).unwrap();
                tx.0
                    .send(box payload::Packet::Evict {
                        node: None,
                        num_keys: 1,
                    })
                    .unwrap();
            }

            let msg = CoordinationMessage {
                source: self.sender_addr,
                epoch: self.epoch,
                payload: CoordinationPayload::Heartbeat,
            };
            match self.sender.send(msg) {
                Err(_) => unimplemented!(),
                Ok(_) => {
                    self.last_heartbeat = Instant::now();
                    self.heartbeat_every
                }
            }
        } else {
            self.heartbeat_every - elapsed
        }
    }

    fn reads_listen(
        addr: SocketAddr,
        readers: Readers,
        reader_threads: usize,
    ) -> (PoolHandle<()>, SocketAddr) {
        let listener = TcpListener::bind(&addr).unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = PoolBuilder::from(listener).unwrap();
        let h = pool.with_state(readers.clone())
            .with_adapter(RpcServiceEndpoint::new)
            .run(
                reader_threads,
                |conn: &mut readers::Rpc, s: &mut Readers| loop {
                    match conn.try_recv() {
                        Ok(m) => {
                            readers::handle_message(m, conn, s);
                        }
                        Err(TryRecvError::Empty) => break Ok(false),
                        Err(TryRecvError::DeserializationError(e)) => {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                        }
                        Err(TryRecvError::Disconnected) => break Ok(true),
                    }
                },
            );

        (h, addr)
    }

    pub(super) fn shutdown(mut self) {
        self.worker_pool.wait();
        self.read_threads.finish();
    }
}
