use channel;
use channel::tcp::{TcpSender, TryRecvError};
use channel::rpc::RpcServiceEndpoint;
use consensus::Epoch;
use dataflow::{DomainBuilder, Readers};
use dataflow::prelude::{ChannelCoordinator, DomainIndex};

use controller::ControllerState;
use coordination::{CoordinationMessage, CoordinationPayload};
use worker;

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
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

    log: slog::Logger,
}

impl WorkerInner {
    pub(super) fn new(
        listen_addr: IpAddr,
        checktable_addr: SocketAddr,
        controller_addr: SocketAddr,
        souplet_addr: SocketAddr,
        state: &ControllerState,
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
            state.config.nreaders,
        );

        let mut sender = match TcpSender::connect(&controller_addr, None) {
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
                state.config.nworkers,
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
            log,
        })
    }

    pub(super) fn handle_domain_assign(
        &mut self,
        d: DomainBuilder,
    ) -> Result<(), channel::tcp::SendError> {
        let listener = ::std::net::TcpListener::bind(SocketAddr::new(self.listen_addr, 0)).unwrap();
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
        self.worker_pool
            .add_replica(worker::NewReplica { inner: d, listener });

        // need to register the domain with the local channel coordinator
        self.channel_coordinator
            .insert_addr((idx, shard), addr, false);

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

    pub(super) fn handle_domain_booted(
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
                |conn: &mut ::souplet::readers::Rpc, s: &mut Readers| loop {
                    match conn.try_recv() {
                        Ok(m) => {
                            ::souplet::readers::handle_message(m, conn, s);
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

    pub(crate) fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub(crate) fn shutdown(mut self) {
        self.worker_pool.wait();
        self.read_threads.finish();
    }
}
