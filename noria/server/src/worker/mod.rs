use crate::coordination::{
    do_noria_rpc, DomainDescriptor, HeartbeatPayload, RegisterPayload, RunDomainResponse,
};
use crate::errors::internal_err;
use crate::worker::replica::WrappedDomainRequest;
use crate::{ReadySetResult, VolumeId};
use dataflow::{DomainBuilder, DomainRequest, Packet, Readers};
use futures_util::{future::TryFutureExt, sink::SinkExt, stream::StreamExt};
use launchpad::select;
use metrics::{counter, gauge};
use noria::internal::DomainIndex;
use noria::metrics::recorded;
use noria::{channel, ReadySetError};
use replica::ReplicaAddr;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::task::{Context, Poll};
use std::time::Duration;
use stream_cancel::Valve;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Interval;
use url::Url;

pub(crate) mod readers;
mod replica;

type ChannelCoordinator = channel::ChannelCoordinator<ReplicaAddr, Box<Packet>>;

/// Some kind of request for a running Noria worker.
///
/// Most of these requests return `()`, apart from `DomainRequest`.
#[derive(Clone, Serialize, Deserialize)]
pub enum WorkerRequestKind {
    /// A new controller has been elected.
    NewController {
        /// The URI of the new controller.
        controller_uri: Url,
        /// The period at which the new controller expects heartbeat packets.
        heartbeat_every: Duration,
    },

    /// A new domain should be started on this worker.
    RunDomain(DomainBuilder),

    /// A set of domains has been started elsewhere in the distributed system.
    ///
    /// The message contains information on how the domain can be reached, in order that
    /// domains on this worker can communicate with it.
    GossipDomainInformation(Vec<DomainDescriptor>),

    /// The sender of the request would like one of the domains on this worker to do something.
    ///
    /// This actually might return something that isn't `()`; see the `DomainRequest` docs
    /// for more.
    DomainRequest {
        /// The index of the target domain.
        target_idx: DomainIndex,
        /// The shard of the target domain.
        target_shard: usize,
        /// The actual request.
        request: Box<DomainRequest>, // box for perf (clippy::large-enum-variant)
    },

    /// Sent to validate that a connection actually works. Provokes an empty response.
    Ping,
}

/// A request to a running Noria worker, containing a request kind and a completion channel.
pub struct WorkerRequest {
    /// The kind of request.
    pub kind: WorkerRequestKind,
    /// A channel through which the result of executing the request should be sent.
    ///
    /// If the request returned a non-`()` response, the result will be sent as a serialized
    /// bincode vector.
    pub done_tx: oneshot::Sender<ReadySetResult<Option<Vec<u8>>>>,
}

/// Information stored in the worker about what the currently active controller is.
#[derive(Clone, Debug)]
pub struct WorkerElectionState {
    /// The URI of the currently active controller.
    controller_uri: Url,
}

pub struct DomainHandle {
    req_tx: Sender<WrappedDomainRequest>,
    join_handle: JoinHandle<Result<(), anyhow::Error>>,
}

/// A Noria worker, responsible for executing some domains.
pub struct Worker {
    /// `slog` logging thingie.
    pub(crate) log: slog::Logger,
    /// The current election state, if it exists (see the `WorkerElectionState` docs).
    pub(crate) election_state: Option<WorkerElectionState>,
    /// A timer for sending heartbeats to the controller.
    pub(crate) heartbeat_interval: Interval,
    /// A timer for doing evictions.
    pub(crate) evict_interval: Option<Interval>,
    /// A memory limit for state, in bytes.
    pub(crate) memory_limit: Option<usize>,
    /// Channel through which worker requests are received.
    pub(crate) rx: Receiver<WorkerRequest>,
    /// Channel coordinator (used by domains to figure out where other domains are).
    pub(crate) coord: Arc<ChannelCoordinator>,
    /// `reqwest` client (used to make HTTP requests to the controller)
    pub(crate) http: reqwest::Client,
    /// The URI of the worker's HTTP server.
    pub(crate) worker_uri: Url,
    /// The IP address to bind on for domain<->domain traffic.
    pub(crate) domain_bind: IpAddr,
    /// The IP address to expose to other domains for domain<->domain traffic.
    pub(crate) domain_external: IpAddr,
    /// The address the server instance is listening on for reads.
    pub(crate) reader_addr: SocketAddr,
    /// A store of the current state size of each domain, used for eviction purposes.
    pub(crate) state_sizes: Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
    /// Read handles.
    pub(crate) readers: Readers,
    /// Valve for shutting down; triggered by the [`Handle`] when [`Handle::shutdown`] is called.
    pub(crate) valve: Valve,
    /// The region this worker is in.
    pub(crate) region: Option<String>,
    /// Whether or not this worker is used only to hold reader domains.
    pub(crate) reader_only: bool,

    /// Handles to domains currently being run by this worker.
    ///
    /// These are indexed by (domain index, shard).
    pub(crate) domains: HashMap<(DomainIndex, usize), DomainHandle>,

    /// Volume id associated with the server the worker is running on.
    pub(crate) volume_id: Option<VolumeId>,
}

impl Worker {
    async fn process_heartbeat(&mut self) {
        if let Some(wes) = self.election_state.as_ref() {
            #[allow(clippy::unwrap_used)] // This is given a known string, so can't fail
            let uri = wes.controller_uri.join("/worker_rx/heartbeat").unwrap();
            #[allow(clippy::unwrap_used)] // This is given a known-good value, so can't fail
            let body = bincode::serialize(&HeartbeatPayload {
                worker_uri: self.worker_uri.clone(),
            })
            .unwrap();
            let log = self.log.clone();
            // this happens in a background task to avoid deadlocks
            tokio::spawn(
                do_noria_rpc::<()>(self.http.post(uri).body(body))
                    .map_err(move |e| warn!(log, "heartbeat failed: {}", e)),
            );
        }
    }

    fn process_eviction(&mut self) {
        tokio::spawn(do_eviction(
            self.log.clone(),
            self.memory_limit,
            self.coord.clone(),
            self.state_sizes.clone(),
        ));
    }

    async fn process_worker_request(&mut self, req: WorkerRequest) {
        let ret = self.handle_worker_request(req.kind).await;
        if let Err(ref e) = ret {
            warn!(self.log, "worker request failed: {}", e.to_string());
        }
        // discard result, since Err(..) means "the receiving end was dropped"
        let _ = req.done_tx.send(ret);
    }

    async fn handle_worker_request(
        &mut self,
        req: WorkerRequestKind,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        match req {
            WorkerRequestKind::NewController {
                controller_uri,
                heartbeat_every,
            } => {
                info!(
                    self.log,
                    "worker informed of new controller at {}", controller_uri
                );
                let log = self.log.clone();
                tokio::spawn(
                    do_noria_rpc::<()>(
                        self.http
                            .post(controller_uri.join("/worker_rx/register")?)
                            .body(bincode::serialize(&RegisterPayload {
                                worker_uri: self.worker_uri.clone(),
                                reader_addr: self.reader_addr,
                                region: self.region.clone(),
                                reader_only: self.reader_only,
                                volume_id: self.volume_id.clone(),
                            })?),
                    )
                    .map_err(move |e| {
                        warn!(log, "controller registration failed: {}", e);
                    }),
                );
                self.domains.clear();
                for (_, d) in self.domains.drain() {
                    d.join_handle.abort();
                }
                self.election_state = Some(WorkerElectionState { controller_uri });
                self.heartbeat_interval = tokio::time::interval(heartbeat_every);
                Ok(None)
            }
            WorkerRequestKind::RunDomain(builder) => {
                let idx = builder.index;
                let shard = builder.shard.unwrap_or(0);

                info!(self.log, "received domain {}.{} to run", idx.index(), shard);

                let bind_on = self.domain_bind;
                let listener = tokio::net::TcpListener::bind(&SocketAddr::new(bind_on, 0))
                    .map_err(|e| {
                        internal_err(format!(
                            "failed to bind domain {}.{} on {}: {}",
                            idx.index(),
                            shard,
                            bind_on,
                            e
                        ))
                    })
                    .await?;
                let bind_actual = listener.local_addr().map_err(|e| {
                    internal_err(format!("couldn't get TCP local address for domain: {}", e))
                })?;
                let mut bind_external = bind_actual;
                bind_external.set_ip(self.domain_external);

                let state_size = Arc::new(AtomicUsize::new(0));
                let domain = builder.build(
                    self.log.clone(),
                    self.readers.clone(),
                    self.coord.clone(),
                    state_size.clone(),
                );

                // this channel is used for in-process domain traffic, to avoid going through the
                // network stack unnecessarily
                let (local_tx, local_rx) = tokio::sync::mpsc::unbounded_channel();
                // this channel is used for domain requests; it has a buffer size of 1 to prevent
                // flooding a domain with requests
                let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

                // need to register the domain with the local channel coordinator.
                // local first to ensure that we don't unnecessarily give away remote for a
                // local thing if there's a race
                self.coord.insert_local((idx, shard), local_tx)?;
                self.coord.insert_remote((idx, shard), bind_external)?;

                tokio::task::block_in_place(|| {
                    self.state_sizes
                        .lock()
                        .unwrap()
                        .insert((idx, shard), state_size)
                });

                let replica = replica::Replica::new(
                    domain,
                    listener,
                    local_rx,
                    request_rx,
                    self.log.clone(),
                    self.coord.clone(),
                );

                let jh = tokio::spawn(replica.run());

                self.domains.insert(
                    (idx, shard),
                    DomainHandle {
                        req_tx: request_tx,
                        join_handle: jh,
                    },
                );

                info!(
                    self.log,
                    "domain {}.{} booted; binds on {} and exposes {} to others",
                    idx.index(),
                    shard,
                    bind_actual,
                    bind_external
                );
                let resp = RunDomainResponse {
                    external_addr: bind_external,
                };
                Ok(Some(bincode::serialize(&resp)?))
            }
            WorkerRequestKind::GossipDomainInformation(domains) => {
                for dd in domains {
                    let domain = dd.domain();
                    let shard = dd.shard();
                    let addr = dd.addr();
                    trace!(
                        self.log,
                        "found that domain {}.{} is at {:?}",
                        domain.index(),
                        shard,
                        addr
                    );
                    self.coord.insert_remote((domain, shard), addr)?;
                }
                Ok(None)
            }
            WorkerRequestKind::DomainRequest {
                target_idx,
                target_shard,
                request,
            } => {
                let nsde = || ReadySetError::NoSuchDomain {
                    domain_index: target_idx.index(),
                    shard: target_shard,
                };
                let dh = self
                    .domains
                    .get_mut(&(target_idx, target_shard))
                    .ok_or_else(nsde)?;
                let (tx, rx) = oneshot::channel();
                let _ = dh
                    .req_tx
                    .send(WrappedDomainRequest {
                        req: *request,
                        done_tx: tx,
                    })
                    .await
                    .map_err(|_| nsde())?;
                rx.await.map_err(|_| nsde())?
            }
            WorkerRequestKind::Ping => Ok(None),
        }
    }

    /// Run the worker continuously, processing worker requests, heartbeats, and domain failures.
    ///
    /// This function returns if the worker request sender is dropped.
    pub async fn run(mut self) {
        loop {
            fn poll_domains<'a>(
                domains: &'a mut HashMap<(DomainIndex, usize), DomainHandle>,
            ) -> impl Future<
                Output = Vec<(
                    DomainIndex,
                    usize,
                    Result<Result<(), anyhow::Error>, tokio::task::JoinError>,
                )>,
            > + 'a {
                // poll all currently running domains, futures 0.1 style!
                futures_util::future::poll_fn(move |cx: &mut Context<'_>| {
                    let mut failed_domains = vec![];
                    for ((idx, shard), dh) in domains.iter_mut() {
                        if let Poll::Ready(ret) = Pin::new(&mut dh.join_handle).poll(cx) {
                            // uh oh, a domain failed!
                            failed_domains.push((*idx, *shard, ret));
                        }
                    }
                    if failed_domains.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(failed_domains)
                    }
                })
            }

            // produces a value when the `Valve` is closed
            let mut shutdown_stream = self.valve.wrap(futures_util::stream::pending::<()>());

            let ei = &mut self.evict_interval;
            let eviction = async {
                if let Some(ref mut ei) = ei {
                    ei.tick().await
                } else {
                    futures_util::future::pending().await
                }
            };

            select! {
                req = self.rx.recv() => {
                    if let Some(req) = req {
                        self.process_worker_request(req).await;
                    }
                    else {
                        info!(self.log, "worker shutting down after request handle dropped");
                        return;
                    }
                }
                failed_domains = poll_domains(&mut self.domains) => {
                    for (idx, shard, err) in failed_domains {
                        // FIXME(eta): do something about this, now that we can?
                        error!(self.log, "domain {}.{} failed: {:?}", idx.index(), shard, err);
                        self.domains.remove(&(idx, shard));
                    }
                }
                _ = shutdown_stream.next() => {
                    info!(self.log, "worker shutting down after valve shut");
                    return;
                }
                _ = eviction => {
                    self.process_eviction();
                }
                _ = self.heartbeat_interval.tick() => {
                    self.process_heartbeat().await;
                }
            }
        }
    }
}

#[allow(clippy::type_complexity)]
async fn do_eviction(
    log: slog::Logger,
    memory_limit: Option<usize>,
    coord: Arc<ChannelCoordinator>,
    state_sizes: Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
) -> ReadySetResult<()> {
    let mut domain_senders = HashMap::new();

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
    gauge!(
        recorded::EVICTION_WORKER_PARTIAL_MEMORY_BYTES_USED,
        total as f64
    );
    match memory_limit {
        None => Ok(()),
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
                sizes.sort_unstable_by_key(|&(_, s)| -(s as i64));
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
                        recorded::EVICTION_WORKER_EVICTIONS_REQUESTED,
                        1,
                        "domain" => target.0.index().to_string(),
                    );

                    let tx = match domain_senders.entry(target) {
                        Occupied(entry) => entry.into_mut(),
                        Vacant(entry) => entry.insert(tokio::task::block_in_place(|| {
                            coord.builder_for(&target)?.build_async().map_err(|e| {
                                internal_err(format!(
                                    "an error occurred while trying to create a domain connection: '{}'",
                                    e
                                ))
                            })
                        })?),
                    };
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
            Ok(())
        }
    }
}
