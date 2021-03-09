use crate::controller::ControllerState;
use crate::coordination::{CoordinationMessage, CoordinationPayload, DomainDescriptor};
use crate::startup::Event;
use async_bincode::AsyncBincodeWriter;
use dataflow::{DomainBuilder, Packet};
use futures_util::{future::FutureExt, future::TryFutureExt, sink::SinkExt, stream::StreamExt};
use metrics::{counter, gauge};
use noria::channel;
use noria::consensus::Epoch;
use noria::internal::DomainIndex;
use noria::ControllerDescriptor;
use replica::ReplicaAddr;
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
use tokio::sync::mpsc::UnboundedSender;

mod readers;
mod replica;

type ChannelCoordinator = channel::ChannelCoordinator<ReplicaAddr, Box<Packet>>;

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
pub(super) async fn main(
    alive: tokio::sync::mpsc::Sender<()>,
    mut worker_rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    listen_addr: IpAddr,
    external_addr: IpAddr,
    waddr: SocketAddr,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    log: slog::Logger,
) {
    // shared df state
    let coord = Arc::new(ChannelCoordinator::new());

    let mut worker_state = InstanceState::Pining;
    let log = log.clone();
    while let Some(e) = worker_rx.next().await {
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
                            add_domain.send(d).unwrap_or_else(|d| {
                                panic!("could not add new domain {:?}", d);
                            });
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
                let (rep_tx, rep_rx) = tokio::sync::mpsc::unbounded_channel();
                let ctrl = listen_df(
                    alive.clone(),
                    valve,
                    log.clone(),
                    (memory_limit, memory_check_frequency),
                    &state,
                    &descriptor,
                    waddr,
                    coord.clone(),
                    listen_addr,
                    external_addr,
                    rep_rx,
                )
                .await;

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
    }

    // shutting down...
    //
    // NOTE: the Trigger in InstanceState::Active is dropped when the for_each
    // closure above is dropped, which will also shut down the worker.
    //
    // TODO: maybe flush things or something?
}

async fn listen_df<'a>(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    log: slog::Logger,
    (memory_limit, evict_every): (Option<usize>, Option<Duration>),
    state: &'a ControllerState,
    desc: &'a ControllerDescriptor,
    waddr: SocketAddr,
    coord: Arc<ChannelCoordinator>,
    on: IpAddr,
    external_addr: IpAddr,
    mut replicas: tokio::sync::mpsc::UnboundedReceiver<DomainBuilder>,
) -> Result<(), anyhow::Error> {
    // first, try to connect to controller
    let ctrl = tokio::net::TcpStream::connect(&desc.worker_addr).await?;
    let ctrl_addr = ctrl.local_addr()?;
    info!(log, "connected to controller"; "src" => ?ctrl_addr);

    let log_prefix = state.config.persistence.log_prefix.clone();
    let prefix = format!("{}-log-", log_prefix);
    let log_files: Vec<String> = fs::read_dir(".")
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().ok().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.path().to_string_lossy().into_owned())
        .filter(|path| path.starts_with(&prefix))
        .collect();

    // extract important things from state config
    let epoch = state.epoch;
    let heartbeat_every = state.config.heartbeat_every;

    let (ctrl_tx, mut ctrl_rx) = tokio::sync::mpsc::unbounded_channel();

    // reader setup
    let readers = Arc::new(Mutex::new(HashMap::new()));
    let rport = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0)).await?;
    let mut raddr = rport.local_addr()?;
    info!(log, "listening for reads"; "on" => ?raddr);
    raddr.set_ip(external_addr);

    // start controller message handler
    let mut ctrl = AsyncBincodeWriter::from(ctrl).for_async();
    let a = alive.clone();
    tokio::spawn(async move {
        let _alive = a;
        while let Some(cm) = ctrl_rx.next().await {
            if let Err(e) = ctrl
                .send(CoordinationMessage {
                    source: ctrl_addr,
                    payload: cm,
                    epoch,
                })
                .await
            {
                // if the controller goes away, another will be elected, and the worker will be
                // restarted, so there's no reason to do anything too drastic here.
                eprintln!("controller went away: {:?}", e);
            }
        }
    });

    // also start readers
    tokio::spawn(readers::listen(
        alive.clone(),
        valve.clone(),
        rport,
        readers.clone(),
    ));

    // and tell the controller about us
    let mut timer = valve.wrap(tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_every,
        heartbeat_every,
    ));
    let a = alive.clone();
    let ctx = ctrl_tx.clone();
    tokio::spawn(async move {
        let _alive = a;
        let _ = ctx.send(CoordinationPayload::Register {
            addr: waddr,
            read_listen_addr: raddr,
            log_files,
        });

        // start sending heartbeats
        while let Some(_) = timer.next().await {
            if let Err(_) = ctx.send(CoordinationPayload::Heartbeat) {
                // if we error we're probably just shutting down
                break;
            }
        }
    });

    let state_sizes = Arc::new(Mutex::new(HashMap::new()));
    if let Some(evict_every) = evict_every {
        let log = log.clone();
        let coord = coord.clone();
        let mut domain_senders = HashMap::new();
        let state_sizes = state_sizes.clone();
        let mut timer = valve.wrap(tokio::time::interval_at(
            tokio::time::Instant::now() + evict_every,
            evict_every,
        ));
        let a = alive.clone();
        tokio::spawn(async move {
            let _alive = a;
            while let Some(_) = timer.next().await {
                do_eviction(
                    &log,
                    memory_limit,
                    &mut domain_senders,
                    &coord,
                    &state_sizes,
                )
                .await;
            }
        });
    }

    // Now we're ready to accept new domains.
    let dcaddr = desc.domain_addr;
    tokio::spawn(
        async move {
            let alive = alive;
            while let Some(d) = replicas.next().await {
                let idx = d.index;
                let shard = d.shard.unwrap_or(0);

                let on = tokio::net::TcpListener::bind(&SocketAddr::new(on, 0)).await?;
                let mut addr = on.local_addr()?;
                info!(log, "listening for domains"; "on" => ?addr);
                addr.set_ip(external_addr);

                let state_size = Arc::new(AtomicUsize::new(0));
                let d = tokio::task::block_in_place(|| {
                    d.build(
                        log.clone(),
                        readers.clone(),
                        coord.clone(),
                        dcaddr,
                        &valve,
                        state_size.clone(),
                    )
                });

                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

                // need to register the domain with the local channel coordinator.
                // local first to ensure that we don't unnecessarily give away remote for a
                // local thing if there's a race
                coord.insert_local((idx, shard), tx);
                coord.insert_remote((idx, shard), addr);

                tokio::task::block_in_place(|| {
                    state_sizes.lock().unwrap().insert((idx, shard), state_size)
                });

                let replica = replica::Replica::new(
                    &valve,
                    d,
                    on,
                    external_addr,
                    rx,
                    ctrl_tx.clone(),
                    log.clone(),
                    coord.clone(),
                );
                let a = alive.clone();
                tokio::spawn(async move {
                    let _alive = a;
                    let log = replica.log.clone();
                    if let Err(e) = replica.await {
                        crit!(log, "replica anyhow: {:?}", e);
                    }
                });

                info!(
                    log,
                    "informed controller that domain {}.{} is at {:?}",
                    idx.index(),
                    shard,
                    addr
                );

                ctrl_tx
                    .send(CoordinationPayload::DomainBooted(DomainDescriptor::new(
                        idx, shard, addr,
                    )))
                    .map_err(|_| {
                        // controller went away -- exit?
                        io::Error::new(io::ErrorKind::Other, "controller went away")
                    })?;
            }

            Ok(())
        }
        .map_err(|e: io::Error| panic!("{:?}", e))
        .map(|_| ()),
    );

    Ok(())
}

#[allow(clippy::type_complexity)]
async fn do_eviction(
    log: &slog::Logger,
    memory_limit: Option<usize>,
    domain_senders: &mut HashMap<
        (DomainIndex, usize),
        Box<dyn futures_sink::Sink<Box<Packet>, Error = Box<bincode::ErrorKind>> + Send + Unpin>,
    >,
    coord: &ChannelCoordinator,
    state_sizes: &Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
) {
    use std::cmp;

    // 2. add current state sizes (could be out of date, as packet sent below is not
    //    necessarily received immediately)
    let mut sizes: Vec<((DomainIndex, usize), usize)> = tokio::task::block_in_place(|| {
        let state_sizes = state_sizes.lock().unwrap();
        state_sizes
            .iter()
            .map(|(ds, sa)| {
                let size = sa.load(Ordering::Acquire);
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
    gauge!("eviction_worker.partial_memory_used_bytes", total as f64);
    match memory_limit {
        None => (),
        Some(limit) => {
            if total >= limit {
                let mut over = total - limit;

                // we are! time to evict.
                // here's how we're going to proceed.
                // we don't want to _empty_ any views if we can avoid it.
                // and we also need to be aware that evicting something from one place may cause a
                // number of downstream evictions.

                // we want to spread the eviction impact across multiple nodes where possible,
                // so we distribute how much we're over the limit across the 3 largest nodes.
                // -1* so we sort in descending order
                // TODO: be smarter than 3 here
                sizes.sort_unstable_by_key(|&(_, s)| -1 * (s as i64));
                sizes.truncate(3);

                // don't evict from tiny things (< 10% of max)
                if let Some(too_small_i) = sizes.iter().position(|&(_, s)| s < sizes[0].1 / 10) {
                    // everything beyond this is smaller, so also too small
                    sizes.truncate(too_small_i);
                }

                // starting with the smallest of the n domains
                let mut n = sizes.len();
                for &(target, size) in sizes.iter().rev() {
                    // TODO: should this be evenly divided, or weighted by the size of the domains?
                    let share = (over + n - 1) / n;
                    // we're only willing to evict at most half the state in each domain
                    // unless this is the only domain left to evict from
                    let evict = if n > 1 {
                        cmp::min(size / 2, share)
                    } else {
                        assert_eq!(share, over);
                        share
                    };
                    over -= evict;
                    n -= 1;

                    debug!(
                            log,
                            "memory footprint ({} bytes) exceeds limit ({} bytes); evicting from largest domain {}",
                            total,
                            limit,
                            target.0.index(),
                        );

                    counter!(
                        "eviction_worker.evictions_requested",
                        1,
                        "domain" => target.0.index().to_string(),
                    );

                    let tx = domain_senders.entry(target).or_insert_with(|| {
                        tokio::task::block_in_place(|| {
                            coord.builder_for(&target).unwrap().build_async().unwrap()
                        })
                    });
                    let r = tx
                        .send(Box::new(Packet::Evict {
                            node: None,
                            num_bytes: evict,
                        }))
                        .await;

                    if let Err(e) = r {
                        // probably exiting?
                        warn!(log, "failed to evict from {}: {}", target.0.index(), e);
                        // remove sender so we don't try to use it again
                        domain_senders.remove(&target);
                    }
                }
            }
        }
    }
}
