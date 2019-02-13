use crate::controller::ControllerState;
use crate::coordination::{CoordinationMessage, CoordinationPayload, DomainDescriptor};
use crate::startup::Event;
use async_bincode::AsyncBincodeWriter;
use dataflow::{DomainBuilder, Packet};
use futures::sync::mpsc::UnboundedSender;
use futures::{self, Future, Sink, Stream};
use noria::channel::{self, TcpSender};
use noria::consensus::Epoch;
use noria::internal::DomainIndex;
use noria::ControllerDescriptor;
use replica::ReplicaIndex;
use slog;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{self, Duration};
use stream_cancel::{Trigger, Valve};
use tokio;
use tokio::prelude::future::Either;
use tokio::prelude::*;
use tokio_io_pool;

mod readers;
mod replica;

type ChannelCoordinator = channel::ChannelCoordinator<ReplicaIndex, Box<Packet>>;

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
pub(super) fn main(
    ioh: tokio_io_pool::Handle,
    worker_rx: futures::sync::mpsc::UnboundedReceiver<Event>,
    listen_addr: IpAddr,
    waddr: SocketAddr,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    log: slog::Logger,
) -> impl Future<Item = (), Error = ()> {
    // shared df state
    let coord = Arc::new(ChannelCoordinator::new());

    let mut worker_state = InstanceState::Pining;
    let log = log.clone();
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
                        "leader listening on external address {:?}", descriptor.external_addr
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
        .and_then(|()| {
            // shutting down...
            //
            // NOTE: the Trigger in InstanceState::Active is dropped when the for_each
            // closure above is dropped, which will also shut down the worker.
            //
            // TODO: maybe flush things or something?
            Ok(())
        })
        .map_err(|e| panic!("{:?}", e))
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
    tokio::spawn(readers::listen(&valve, ioh, rport, readers.clone()));

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

                    crate::block_on(|| {
                        state_sizes.lock().unwrap().insert((idx, shard), state_size)
                    });

                    tokio::spawn(replica::Replica::new(
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

#[allow(clippy::type_complexity)]
fn do_eviction(
    log: &slog::Logger,
    memory_limit: Option<usize>,
    domain_senders: &mut HashMap<(DomainIndex, usize), TcpSender<Box<Packet>>>,
    state_sizes: &Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
) -> impl Future<Item = (), Error = ()> {
    use std::cmp;

    // 2. add current state sizes (could be out of date, as packet sent below is not
    //    necessarily received immediately)
    let sizes: Vec<((DomainIndex, usize), usize)> = crate::block_on(|| {
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
                (*ds, size)
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
                crate::block_on(|| {
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
