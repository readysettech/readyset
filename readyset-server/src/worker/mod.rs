use std::cmp;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use dataflow::payload::EvictRequest;
use dataflow::{DomainBuilder, DomainRequest, Packet, Readers};
use enum_kinds::EnumKind;
use futures::stream::FuturesUnordered;
use futures_util::future::TryFutureExt;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use metrics::{counter, gauge, histogram};
use pin_project::pin_project;
use readyset_alloc::StdThreadBuildWrapper;
use readyset_client::internal::ReplicaAddress;
use readyset_client::metrics::recorded;
use readyset_client::ReadySetHandle;
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use readyset_util::select;
use readyset_util::shutdown::ShutdownReceiver;
use serde::{Deserialize, Serialize};
use tikv_jemalloc_ctl::stats::allocated_mib;
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Interval;
use tracing::{debug, error, info, info_span, trace, warn};
use url::Url;
use vec1::Vec1;

use self::replica::Replica;
use crate::coordination::{DomainDescriptor, RunDomainResponse};
use crate::worker::replica::WrappedDomainRequest;

/// Request handlers and utilities for reading from the ReadHandle of a
/// left-right map associated with a reader node.
pub mod readers;
mod replica;

type ChannelCoordinator = dataflow::ChannelCoordinator<ReplicaAddress, Box<Packet>>;

/// Timeout for requests made from the controller to the server
const CONTROLLER_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Some kind of request for a running ReadySet worker.
///
/// Most of these requests return `()`, apart from `DomainRequest`.
#[derive(Clone, EnumKind, Serialize, Deserialize)]
#[enum_kind(WorkerRequestType)]
pub enum WorkerRequestKind {
    /// A new controller has been elected.
    NewController {
        /// The URI of the new controller.
        controller_uri: Url,
    },

    /// A new domain should be started on this worker.
    RunDomain(DomainBuilder),

    /// Clear domains.
    ClearDomains,

    /// Kill one or more domains running on this worker
    KillDomains(Vec1<ReplicaAddress>),

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
        /// The address of the target domain replica
        replica_address: ReplicaAddress,
        /// The actual request.
        request: Box<DomainRequest>, // box for perf (clippy::large-enum-variant)
    },

    /// Sent to validate that a connection actually works. Provokes an empty response.
    Ping,

    /// Set the memory limit for this worker
    SetMemoryLimit {
        /// The period with which eviction check will be performed
        period: Option<Duration>,
        /// The limit in bytes
        limit: Option<usize>,
    },
}

/// A request to a running ReadySet worker, containing a request kind and a completion channel.
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
#[derive(Clone)]
pub struct WorkerElectionState {
    /// The URI of the currently active controller.
    controller_uri: Url,
    /// A (potentially cached) client for the currently active controller
    controller: Option<ReadySetHandle>,
}

impl WorkerElectionState {
    /// Create a new [`WorkerElectionState`] for the given new controller URL
    fn new(controller_uri: Url) -> Self {
        Self {
            controller_uri,
            controller: None,
        }
    }

    /// Obtain a [`ReadySetHandle`] for the controller we currently know about
    fn handle(
        &mut self,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> &mut ReadySetHandle {
        self.controller.get_or_insert_with(|| {
            ReadySetHandle::make_raw(
                self.controller_uri.clone(),
                request_timeout,
                migration_timeout,
            )
        })
    }
}

/// A handle for sending messages to a domain in-process.
pub struct DomainHandle {
    req_tx: Sender<WrappedDomainRequest>,
    /// Can be used to send an abort signal to the domain
    /// aborts automatically when dropped
    abort: oneshot::Sender<()>,
}

/// Long-lived struct for tracking the currently allocated heap memory used by the current process
/// by querying [`jemalloc_ctl`]
#[derive(Clone, Copy)]
pub struct MemoryTracker {
    epoch: epoch_mib,
    allocated: allocated_mib,
}

impl MemoryTracker {
    /// Construct a new [`MemoryTracker`]
    pub fn new() -> ReadySetResult<Self> {
        Ok(Self {
            epoch: epoch::mib()?,
            allocated: stats::allocated::mib()?,
        })
    }

    /// Query jemalloc for the currently allocated heap memory used by the current process, in
    /// bytes.
    pub fn allocated_bytes(self) -> ReadySetResult<usize> {
        self.epoch.advance()?;
        Ok(self.allocated.read()?)
    }
}

/// A future for a finished domain. Awaiting this future returns a tuple containing the result of
/// the domain running and the domain's [`ReplicaAddress`]
#[pin_project]
struct FinishedDomain(#[pin] JoinHandle<anyhow::Result<()>>, ReplicaAddress);

impl Future for FinishedDomain {
    type Output = (Result<anyhow::Result<()>, JoinError>, ReplicaAddress);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.0.poll(cx));
        Poll::Ready((res, *this.1))
    }
}

fn log_domain_result(domain: ReplicaAddress, result: &Result<anyhow::Result<()>, JoinError>) {
    match result {
        Ok(Ok(())) => info!(%domain, "domain exited"),
        Ok(Err(error)) => error!(%domain, %error, "domain failed with an error"),
        Err(e) if e.is_cancelled() => info!(%domain, "domain future cancelled"),
        Err(error) => error!(%domain, %error, "domain failed with an error"),
    }
}

/// A ReadySet worker, responsible for executing some domains.
pub struct Worker {
    /// The current election state, if it exists (see the `WorkerElectionState` docs).
    election_state: Option<WorkerElectionState>,
    /// A timer for doing evictions.
    evict_interval: Option<Interval>,
    /// A memory limit for state, in bytes.
    memory_limit: Option<usize>,
    /// Channel through which worker requests are received.
    rx: Receiver<WorkerRequest>,
    /// Channel coordinator (used by domains to figure out where other domains are).
    coord: Arc<ChannelCoordinator>,
    /// The IP address to bind on for domain<->domain traffic.
    domain_bind: IpAddr,
    /// The IP address to expose to other domains for domain<->domain traffic.
    domain_external: IpAddr,
    /// A store of the current state size of each domain, used for eviction purposes.
    state_sizes: Arc<Mutex<HashMap<ReplicaAddress, Arc<AtomicUsize>>>>,
    /// Read handles.
    readers: Readers,
    /// Handles to domains currently being run by this worker.
    ///
    /// These are indexed by (domain index, shard).
    domains: HashMap<ReplicaAddress, DomainHandle>,

    memory: MemoryTracker,
    is_evicting: Arc<AtomicBool>,
    domain_wait_queue: FuturesUnordered<FinishedDomain>,
    shutdown_rx: ShutdownReceiver,
}

impl Worker {
    pub fn new(
        worker_rx: Receiver<WorkerRequest>,
        listen_addr: IpAddr,
        external_addr: SocketAddr,
        readers: Readers,
        memory_limit: Option<usize>,
        memory_check_frequency: Option<Duration>,
        shutdown_rx: ShutdownReceiver,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            election_state: None,
            // this initial duration doesn't matter; it gets set upon worker registration
            evict_interval: memory_check_frequency.map(|f| tokio::time::interval(f)),
            memory_limit,
            rx: worker_rx,
            coord: Arc::new(Default::default()),
            domain_bind: listen_addr,
            domain_external: external_addr.ip(),
            state_sizes: Default::default(),
            readers,
            domains: Default::default(),
            memory: MemoryTracker::new()?,
            is_evicting: Default::default(),
            domain_wait_queue: Default::default(),
            shutdown_rx,
        })
    }

    fn process_eviction(&mut self) {
        tokio::spawn(do_eviction(
            self.memory_limit,
            self.coord.clone(),
            self.memory,
            Arc::clone(&self.state_sizes),
            Arc::clone(&self.is_evicting),
        ));
    }

    async fn process_worker_request(&mut self, req: WorkerRequest) {
        let ret = self.handle_worker_request(req.kind).await;
        if let Err(ref e) = ret {
            warn!(error = %e, "worker request failed");
        }
        // discard result, since Err(..) means "the receiving end was dropped"
        let _ = req.done_tx.send(ret);
    }

    async fn handle_worker_request(
        &mut self,
        req: WorkerRequestKind,
    ) -> ReadySetResult<Option<Vec<u8>>> {
        match req {
            WorkerRequestKind::NewController { controller_uri } => {
                info!(%controller_uri, "worker informed of new controller");
                self.election_state = Some(WorkerElectionState::new(controller_uri));
                Ok(None)
            }
            WorkerRequestKind::ClearDomains => {
                info!("controller requested that this worker clears its existing domains");
                self.coord.clear();
                self.domains.clear();
                while let Some((result, domain)) = self.domain_wait_queue.next().await {
                    log_domain_result(domain, &result)
                }

                Ok(None)
            }
            WorkerRequestKind::RunDomain(builder) => {
                let replica_addr = builder.address();
                let span = info_span!("domain", address = %replica_addr);
                span.in_scope(|| debug!("received domain to run"));

                let bind_on = self.domain_bind;
                let listener = tokio::net::TcpListener::bind(&SocketAddr::new(bind_on, 0))
                    .map_err(|e| {
                        internal_err!(
                            "failed to bind domain {} on {}: {}",
                            replica_addr,
                            bind_on,
                            e
                        )
                    })
                    .await?;
                let bind_actual = listener.local_addr().map_err(|e| {
                    internal_err!("couldn't get TCP local address for domain: {}", e)
                })?;
                let mut bind_external = bind_actual;
                bind_external.set_ip(self.domain_external);

                // this channel is used for async persistent state initialization.
                // since domains can have at most one base table, there's no need to have a
                // buffer with a size bigger than one.
                let (init_state_tx, init_state_rx) = tokio::sync::mpsc::channel(1);

                let state_size = Arc::new(AtomicUsize::new(0));
                let domain = builder.build(
                    self.readers.clone(),
                    self.coord.clone(),
                    state_size.clone(),
                    init_state_tx,
                );

                // this channel is used for in-process domain traffic, to avoid going through the
                // network stack unnecessarily
                let (local_tx, local_rx) = tokio::sync::mpsc::unbounded_channel();
                // this channel is used for domain requests; it has a buffer size of 1 to prevent
                // flooding a domain with requests
                let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);

                // need to register the domain with the local channel coordinator.
                // local first to ensure that we don't unnecessarily give away remote for a
                // local thing if there's a race
                self.coord.insert_local(replica_addr, local_tx);
                self.coord.insert_remote(replica_addr, bind_external);

                self.state_sizes
                    .lock()
                    .await
                    .insert(replica_addr, state_size);

                let replica = Replica::new(
                    domain,
                    listener,
                    local_rx,
                    req_rx,
                    init_state_rx,
                    self.coord.clone(),
                );
                // Each domain is single threaded in nature, so we spawn each one in a separate
                // thread, so we can avoid running blocking operations on the multi
                // threaded tokio runtime
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .max_blocking_threads(1)
                    .build()
                    .unwrap();

                let jh = runtime.spawn(replica.run());

                let (abort, abort_rx) = oneshot::channel::<()>();
                // Spawn the actual thread to run the domain
                std::thread::Builder::new()
                    .name(format!("Domain {}", replica_addr))
                    .stack_size(2 * 1024 * 1024) // Use the same value tokio is using
                    .spawn_wrapper(move || {
                        // The runtime will run until the abort signal is sent.
                        // This will happen either if the DomainHandle is dropped (and error is
                        // received) or an actual signal is sent on the
                        // channel
                        let _ = runtime.block_on(abort_rx);
                        runtime.shutdown_background();
                    })?;

                self.domains
                    .insert(replica_addr, DomainHandle { req_tx, abort });

                self.domain_wait_queue
                    .push(FinishedDomain(jh, replica_addr));

                span.in_scope(|| debug!(%bind_actual, %bind_external, "domain booted"));
                let resp = RunDomainResponse {
                    external_addr: bind_external,
                };
                Ok(Some(bincode::serialize(&resp)?))
            }
            WorkerRequestKind::KillDomains(domains) => {
                for addr in domains {
                    match self.domains.remove(&addr) {
                        Some(domain) => {
                            info!(domain = %addr, "Aborting domain at request of controller");
                            let _ = domain.abort.send(());
                        }
                        None => warn!(domain = %addr, "Asked to kill domain that is not running"),
                    }
                }
                Ok(None)
            }
            WorkerRequestKind::GossipDomainInformation(domains) => {
                for dd in domains {
                    trace!(
                        replica_address = %dd.replica_address(),
                        addr = ?dd.socket_address(),
                        "found domain"
                    );
                    self.coord
                        .insert_remote(dd.replica_address(), dd.socket_address());
                }
                Ok(None)
            }
            WorkerRequestKind::DomainRequest {
                replica_address,
                request,
            } => {
                let nsde = || ReadySetError::NoSuchReplica {
                    domain_index: replica_address.domain_index.index(),
                    shard: replica_address.shard,
                    replica: replica_address.replica,
                };
                let dh = self.domains.get_mut(&replica_address).ok_or_else(nsde)?;
                let (tx, rx) = oneshot::channel();
                dh.req_tx
                    .send(WrappedDomainRequest {
                        req: *request,
                        done_tx: tx,
                    })
                    .await
                    .map_err(|_| nsde())?;
                rx.await.map_err(|_| nsde())?
            }
            WorkerRequestKind::Ping => Ok(None),
            WorkerRequestKind::SetMemoryLimit { period, limit } => {
                self.evict_interval = period.map(tokio::time::interval);
                self.memory_limit = limit;
                Ok(None)
            }
        }
    }

    async fn handle_domain_future_completion(
        &mut self,
        addr: ReplicaAddress,
        result: Result<anyhow::Result<()>, JoinError>,
    ) -> ReadySetResult<()> {
        log_domain_result(addr, &result);

        if matches!(result, Err(e) if e.is_cancelled()) {
            return Ok(());
        }

        self.election_state
            .as_mut()
            .ok_or_else(|| internal_err!("Domain {addr} failed when controller is unknown!"))?
            .handle(Some(CONTROLLER_REQUEST_TIMEOUT), None)
            .domain_died(addr)
            .await
    }

    /// Run the worker continuously, processing worker requests, heartbeats, and domain failures.
    ///
    /// This function returns if the worker request sender is dropped.
    pub async fn run(mut self) {
        loop {
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
                        debug!("worker shutting down after request handle dropped");
                        return;
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    debug!("worker shutting down after shutdown signal received");
                    return;
                }
                _ = eviction => {
                    self.process_eviction();
                }
                Some((result, replica_address)) = self.domain_wait_queue.next() => {
                    if let Err(error) = self.handle_domain_future_completion(
                        replica_address,
                        result
                    ).await {
                        // If we can't notify the controller about the death of a domain, we have to
                        // exit, since the cluster can no longer be considered healthy
                        error!(
                            %error,
                            %replica_address,
                            "Error notifying controller about the death of domain"
                        );
                        panic!("Could not notify controller about the death of domain")
                    }
                }
            }
        }
    }
}

/// Calculate the total memory used by the process (by querying [`jemalloc_ctl`]), then perform an
/// eviction if that's over the configured `memory_limit`.
///
/// There is a *significant* proportional discrepancy - about 8x - between the memory size reported
/// by individual node states and the actual number of bytes allocated by the application - rather
/// than trying to make the estimation accurate, we instead use the [`jemalloc_ctl`] API to query
/// the global allocator directly for the amount of memory we use and use that to decide *when* to
/// evict, but use the state sizes of individual nodes to decide *where* to evict. This is
/// imperfect, and should likely be improved in the future, but is a good way to avoid running fully
/// out of memory and getting OOM-killed before we ever realise it's time to evict.
#[allow(clippy::type_complexity)]
async fn do_eviction(
    memory_limit: Option<usize>,
    coord: Arc<ChannelCoordinator>,
    memory_tracker: MemoryTracker,
    state_sizes: Arc<Mutex<HashMap<ReplicaAddress, Arc<AtomicUsize>>>>,
    is_evicting: Arc<AtomicBool>,
) -> ReadySetResult<()> {
    if is_evicting.swap(true, Ordering::Relaxed) {
        // Already evicting, nothing to do
        return Ok(());
    }

    let _guard = scopeguard::guard((), move |_| {
        is_evicting.store(false, Ordering::Relaxed);
    });

    let span = info_span!("evicting");
    let start = std::time::Instant::now();

    let mut used: usize = memory_tracker.allocated_bytes()?;
    gauge!(recorded::EVICTION_WORKER_HEAP_ALLOCATED_BYTES, used as f64);
    // Are we over the limit?
    match memory_limit {
        None => Ok(()),
        Some(limit) => {
            while used >= limit {
                // we are! time to evict.
                // add current state sizes (could be out of date, as packet sent below is not
                // necessarily received immediately)
                let (mut sizes, total_reported) = {
                    let state_sizes = state_sizes.lock().await;
                    let mut total_reported = 0;
                    let sizes = state_sizes
                        .iter()
                        .map(|(replica_addr, size_atom)| {
                            let size = size_atom.load(Ordering::Acquire);
                            span.in_scope(|| {
                                trace!("domain {} state size is {} bytes", replica_addr, size)
                            });
                            total_reported += size;
                            (*replica_addr, size)
                        })
                        .collect::<Vec<_>>();
                    (sizes, total_reported)
                };

                // state sizes are under actual memory usage, but roughly proportional to actual
                // memory usage - let's figure out proportionally how much *reported* memory we
                // should evict
                let actual_over = used - limit;
                let mut proportional_over =
                    ((total_reported as f64 / used as f64) * actual_over as f64).round() as usize;
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
                let mut domain_senders = HashMap::new();
                for &(target, size) in sizes.iter().rev() {
                    // TODO: should this be evenly divided, or weighted by the size of the domains?
                    let share = (proportional_over + n - 1) / n;
                    // we're only willing to evict at most half the state in each domain
                    // unless this is the only domain left to evict from
                    let evict = if n > 1 {
                        cmp::min(size / 2, share)
                    } else {
                        assert_eq!(share, proportional_over);
                        share
                    };
                    proportional_over -= evict;
                    n -= 1;

                    span.in_scope(|| {
                        debug!(
                            "memory footprint ({} bytes) exceeds limit ({} bytes); evicting from largest domain {}",
                            used,
                            limit,
                            target.domain_index,
                        )
                    });

                    counter!(
                        recorded::EVICTION_WORKER_EVICTIONS_REQUESTED,
                        1,
                        "domain" => target.domain_index.index().to_string(),
                    );

                    let tx = match domain_senders.entry(target) {
                        Occupied(entry) => entry.into_mut(),
                        Vacant(entry) => entry.insert(tokio::task::block_in_place(|| {
                            coord.builder_for(&target)?.build_async().map_err(|e| {
                                internal_err!(
                                    "an error occurred while trying to create a domain connection: '{}'",
                                    e
                                )
                            })
                        })?),
                    };
                    let r = tx
                        .send(Box::new(Packet::Evict(EvictRequest::Bytes {
                            node: None,
                            num_bytes: evict,
                        })))
                        .await;

                    if let Err(e) = r {
                        // probably exiting?
                        span.in_scope(|| {
                            warn!(
                                "failed to evict from {}: {}",
                                target.domain_index.index(),
                                e
                            )
                        });
                        // remove sender so we don't try to use it again
                        domain_senders.remove(&target);
                    }
                }
                // Check again the allocated memory size
                used = memory_tracker.allocated_bytes()?;
            }
            histogram!(
                recorded::EVICTION_WORKER_EVICTION_TIME,
                start.elapsed().as_micros() as f64,
            );

            Ok(())
        }
    }
}

impl Drop for Worker {
    /// This is only implemented for the sake of RocksDB that doesn't really
    /// like having its thread being destroyed while it is still open, so
    /// we need to join the threads nicely. It doesn't really matter that
    /// we do this by spawning a thread and blocking a drop, as the Worker
    /// is only dropped when the process is finished.
    fn drop(&mut self) {
        let Worker {
            domains,
            domain_wait_queue,
            ..
        } = self;

        domains.clear(); // This sends a signal to all the domains on drop

        let mut domain_wait_queue = std::mem::take(domain_wait_queue);

        let rt = tokio::runtime::Handle::current();

        std::thread::Builder::new()
            .name("Worker Drop".to_string())
            .spawn_wrapper(move || {
                rt.block_on(async move {
                    while let Some((handle, replica_addr)) = domain_wait_queue.next().await {
                        match handle {
                            Ok(Err(e)) => {
                                error!(domain = %replica_addr, err = %e, "domain failed during drop")
                            }
                            Err(e) if !e.is_cancelled() => {
                                error!(domain = %replica_addr, err = %e, "domain failed during drop")
                            }
                            _ => {}
                        }
                    }
                });
            })
            .expect("failed to register thread")
            .join()
            .expect("This thread shouldn't panic");
    }
}
