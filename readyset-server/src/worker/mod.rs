use std::cmp;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures::stream::FuturesUnordered;
use futures_util::future::TryFutureExt;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use metrics::{counter, gauge, histogram};
use pin_project::pin_project;
use rand::RngExt;
use readyset_sql::ast::Relation;
use serde::{Deserialize, Serialize};
use tikv_jemalloc_ctl::stats::allocated_mib;
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Interval;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use vec1::Vec1;

use crate::worker::replica::WrappedDomainRequest;
use dataflow::payload::{packets::Evict, Eviction};
use dataflow::{
    BarrierCredit, ChannelCoordinator, Domain, DomainBuilder, DomainRequest, Packet, Readers,
};
use readyset_alloc::StdThreadBuildWrapper;
use readyset_client::internal::DomainIndex;
use readyset_client::metrics::recorded;
use readyset_client::TableStatus;
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use readyset_util::shutdown::ShutdownReceiver;
use readyset_util::{select, time_scope};
use replica::Replica;

/// Request handlers and utilities for reading from the ReadHandle of a
/// left-right map associated with a reader node.
pub mod readers;
mod replica;

/// Timeout for logging slow `worker_request` handling
const SLOW_REQUEST_THRESHOLD: Duration = Duration::from_secs(1);

/// Wire shape of a worker request used by the `/worker_request` HTTP route handler
/// in `http_router`. The handler deserialises a `WorkerRequestKind` from the body
/// then bridges into the typed [`WorkerRequest`] channel below. The in-process
/// control path skips this and constructs a [`WorkerRequest`] directly.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerRequestKind {
    RunDomain(DomainBuilder),
    ClearDomains,
    KillDomains(Vec1<DomainIndex>),
    DomainRequest {
        domain_index: DomainIndex,
        request: Box<DomainRequest>, // box for perf (clippy::large-enum-variant)
    },
    SetMemoryLimit {
        period: Option<Duration>,
        limit: Option<usize>,
    },
}

/// A typed request to a running ReadySet worker. Each variant carries the
/// completion channel for that variant's response type, eliminating the bincode
/// round-trip that the HTTP path required.
pub enum WorkerRequest {
    RunDomain {
        builder: DomainBuilder,
        done_tx: oneshot::Sender<ReadySetResult<()>>,
    },
    ClearDomains {
        done_tx: oneshot::Sender<ReadySetResult<()>>,
    },
    KillDomains {
        domains: Vec1<DomainIndex>,
        done_tx: oneshot::Sender<ReadySetResult<()>>,
    },
    DomainRequest {
        domain_index: DomainIndex,
        request: Box<DomainRequest>,
        /// The raw bincoded response payload from the domain (or `bincode::serialize(&())`
        /// for unit-returning variants).
        done_tx: oneshot::Sender<ReadySetResult<Vec<u8>>>,
    },
    SetMemoryLimit {
        period: Option<Duration>,
        limit: Option<usize>,
        done_tx: oneshot::Sender<ReadySetResult<()>>,
    },
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
/// the domain running and the domain's [`DomainIndex`]
#[pin_project]
struct FinishedDomain(#[pin] JoinHandle<ReadySetResult<()>>, DomainIndex);

impl Future for FinishedDomain {
    type Output = (Result<ReadySetResult<()>, JoinError>, DomainIndex);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.0.poll(cx));
        Poll::Ready((res, *this.1))
    }
}

fn report_err<T>(ret: ReadySetResult<T>) -> ReadySetResult<T> {
    if let Err(ref e) = ret {
        warn!(error = %e, "worker request failed");
    }
    ret
}

fn log_domain_result(domain: DomainIndex, result: &Result<ReadySetResult<()>, JoinError>) {
    match result {
        Ok(Ok(())) => info!(%domain, "domain exited"),
        Ok(Err(error)) => error!(%domain, %error, "domain failed with an error"),
        Err(e) if e.is_cancelled() => info!(%domain, "domain future cancelled"),
        Err(error) => error!(%domain, %error, "domain failed with an error"),
    }
}

/// A Readyset worker, responsible for executing some domains.
pub struct Worker {
    /// A timer for doing evictions.
    evict_interval: Interval,
    /// A memory limit for state, in bytes.
    memory_limit: usize,
    /// Channel through which worker requests are received.
    rx: Receiver<WorkerRequest>,
    /// Channel coordinator (used by domains to figure out where other domains are).
    coord: Arc<ChannelCoordinator>,
    /// The IP address to bind on for domain<->domain traffic.
    domain_bind: IpAddr,
    /// The IP address to expose to other domains for domain<->domain traffic.
    domain_external: IpAddr,
    /// Reports domain-task exits to the controller (covers both controller-initiated
    /// teardown and unexpected death). The receiver lives in a dedicated controller-side
    /// task; `UnboundedSender::send` is synchronous and non-blocking, which is what
    /// prevents the historical worker↔controller loopback deadlock.
    domain_exited_tx: UnboundedSender<DomainIndex>,
    /// A store of the current state size of each domain, used for eviction purposes.
    state_sizes: Arc<Mutex<HashMap<DomainIndex, Arc<AtomicUsize>>>>,
    /// Read handles.
    readers: Readers,
    /// Handles to domains currently being run by this worker.
    domains: HashMap<DomainIndex, DomainHandle>,
    /// Run unqueries after a reader eviction.
    unquery: bool,
    memory: MemoryTracker,
    is_evicting: Arc<AtomicBool>,
    domain_wait_queue: FuturesUnordered<FinishedDomain>,
    shutdown_rx: ShutdownReceiver,
    barriers: Arc<BarrierManager>,
    /// Any TableStatus updates sent here will be sent to the current controller.
    table_status_tx: UnboundedSender<(Relation, TableStatus)>,
}

impl Worker {
    const DEFAULT_EVICT_INTERVAL: Duration = Duration::from_millis(100);

    pub fn new(
        rx: Receiver<WorkerRequest>,
        listen_addr: IpAddr,
        external_addr: SocketAddr,
        coord: Arc<ChannelCoordinator>,
        domain_exited_tx: UnboundedSender<DomainIndex>,
        readers: Readers,
        memory_limit: Option<usize>,
        evict_interval: Option<Duration>,
        shutdown_rx: ShutdownReceiver,
        unquery: bool,
        table_status_tx: UnboundedSender<(Relation, TableStatus)>,
    ) -> anyhow::Result<Self> {
        // this initial duration doesn't matter; it gets set upon worker registration
        let evict_interval = evict_interval.unwrap_or(Self::DEFAULT_EVICT_INTERVAL);
        let evict_interval = tokio::time::interval(evict_interval);

        Ok(Self {
            rx,
            domain_bind: listen_addr,
            domain_external: external_addr.ip(),
            coord,
            domain_exited_tx,
            readers,
            memory_limit: memory_limit.unwrap_or(usize::MAX),
            evict_interval,
            shutdown_rx,
            unquery,
            memory: MemoryTracker::new()?,
            state_sizes: Default::default(),
            domains: Default::default(),
            is_evicting: Default::default(),
            domain_wait_queue: Default::default(),
            barriers: Default::default(),
            table_status_tx,
        })
    }

    fn evict_tick(&mut self) {
        tokio::spawn(evict_start(
            self.memory_limit,
            self.coord.clone(),
            self.memory,
            self.state_sizes.clone(),
            self.is_evicting.clone(),
            self.barriers.clone(),
        ));
    }

    async fn process_worker_request(&mut self, req: WorkerRequest) {
        let span = info_span!("readyset_server::worker::process_worker_request");
        let _time = time_scope(span, SLOW_REQUEST_THRESHOLD);
        match req {
            WorkerRequest::ClearDomains { done_tx } => {
                let _ = done_tx.send(report_err(self.clear_domains_impl().await));
            }
            WorkerRequest::RunDomain { builder, done_tx } => {
                let _ = done_tx.send(report_err(self.run_domain_impl(builder).await));
            }
            WorkerRequest::KillDomains { domains, done_tx } => {
                let _ = done_tx.send(report_err(self.kill_domains_impl(domains).await));
            }
            WorkerRequest::DomainRequest {
                domain_index,
                request,
                done_tx,
            } => {
                let _ = done_tx.send(report_err(
                    self.domain_request_impl(domain_index, request).await,
                ));
            }
            WorkerRequest::SetMemoryLimit {
                period,
                limit,
                done_tx,
            } => {
                self.set_memory_limit_impl(period, limit);
                let _ = done_tx.send(Ok(()));
            }
        }
    }

    async fn clear_domains_impl(&mut self) -> ReadySetResult<()> {
        info!("controller requested that this worker clear its existing domains");
        self.coord.clear();
        self.domains.clear();
        while let Some((result, domain)) = self.domain_wait_queue.next().await {
            log_domain_result(domain, &result)
        }
        Ok(())
    }

    async fn run_domain_impl(&mut self, builder: DomainBuilder) -> ReadySetResult<()> {
        let domain = builder.address();
        let span = info_span!("domain", address = %domain);
        span.in_scope(|| debug!("received domain to run"));

        let bind_on = self.domain_bind;
        let listener = tokio::net::TcpListener::bind(&SocketAddr::new(bind_on, 0))
            .map_err(|e| internal_err!("failed to bind domain {} on {}: {}", domain, bind_on, e))
            .await?;
        let bind_actual = listener
            .local_addr()
            .map_err(|e| internal_err!("couldn't get TCP local address for domain: {}", e))?;
        let mut bind_external = bind_actual;
        bind_external.set_ip(self.domain_external);

        // this channel is used for async persistent state initialization.
        // since domains can have at most one base table, there's no need to have a
        // buffer with a size bigger than one.
        let (init_state_tx, init_state_rx) = tokio::sync::mpsc::channel(1);

        let state_size = Arc::new(AtomicUsize::new(0));
        let (dataflow_domain, replay_rx) = builder.build(
            self.readers.clone(),
            self.coord.clone(),
            state_size.clone(),
            init_state_tx,
            self.unquery,
            self.table_status_tx.clone(),
        );

        // this channel is used for in-process domain traffic, to avoid going through the
        // network stack unnecessarily
        let (local_tx, local_rx) = Domain::channel();
        // this channel is used for domain requests; it has a buffer size of 1 to prevent
        // flooding a domain with requests
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);

        // need to register the domain with the local channel coordinator.
        // local first to ensure that we don't unnecessarily give away remote for a
        // local thing if there's a race
        self.coord.insert_local(domain, local_tx);
        self.coord.insert_remote(domain, bind_external);

        self.state_sizes.lock().await.insert(domain, state_size);

        let replica = Replica::new(
            dataflow_domain,
            listener,
            local_rx,
            req_rx,
            init_state_rx,
            replay_rx,
            self.coord.clone(),
            self.barriers.clone(),
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
            .name(format!("Domain {domain}"))
            .stack_size(2 * 1024 * 1024) // Use the same value tokio is using
            .spawn_wrapper(move || {
                // The runtime will run until the abort signal is sent.
                // This will happen either if the DomainHandle is dropped (and error is
                // received) or an actual signal is sent on the
                // channel
                let _ = runtime.block_on(abort_rx);
                runtime.shutdown_background();
            })?;

        self.domains.insert(domain, DomainHandle { req_tx, abort });

        self.domain_wait_queue.push(FinishedDomain(jh, domain));

        span.in_scope(|| debug!(%bind_actual, %bind_external, "domain booted"));
        Ok(())
    }

    async fn kill_domains_impl(&mut self, domains: Vec1<DomainIndex>) -> ReadySetResult<()> {
        for addr in domains {
            let nsde = || ReadySetError::DomainNotFound {
                domain_index: addr.index(),
            };
            match self.domains.remove(&addr) {
                Some(domain) => {
                    info!(domain = %addr, "Shutting down domain");

                    // tell the domain to cleanly shut down. note: this serially shuts down
                    // the domains; it could be done in parallel.
                    let (tx, rx) = oneshot::channel();
                    domain
                        .req_tx
                        .send(WrappedDomainRequest {
                            req: DomainRequest::Shutdown,
                            done_tx: tx,
                        })
                        .await
                        .map_err(|_| nsde())?;
                    let _ = rx.await.map_err(|_| nsde())?;

                    // now, shut down the actual thread for the domain
                    let _ = domain.abort.send(());
                }
                None => warn!(domain = %addr, "Asked to kill domain that is not running"),
            }
        }
        Ok(())
    }

    async fn domain_request_impl(
        &mut self,
        domain_index: DomainIndex,
        request: Box<DomainRequest>,
    ) -> ReadySetResult<Vec<u8>> {
        let nsde = || ReadySetError::DomainNotFound {
            domain_index: domain_index.index(),
        };
        let dh = self.domains.get_mut(&domain_index).ok_or_else(nsde)?;
        let (tx, rx) = oneshot::channel();
        dh.req_tx
            .send(WrappedDomainRequest {
                req: *request,
                done_tx: tx,
            })
            .await
            .map_err(|_| nsde())?;
        let payload = rx.await.map_err(|_| nsde())??;
        // Domain returns `None` for unit-returning variants; the HTTP wire contract
        // serialises `()` for those, so we do the same here to keep the bytes
        // shape callers expect.
        Ok(payload.unwrap_or_else(|| bincode::serialize(&()).expect("serialize ()")))
    }

    fn set_memory_limit_impl(&mut self, period: Option<Duration>, limit: Option<usize>) {
        if let Some(period) = period {
            self.evict_interval = tokio::time::interval(period);
        }
        if let Some(limit) = limit {
            self.memory_limit = limit;
        }
    }

    /// Notify the controller that the domain-task at `addr` has exited (skipping cancellation,
    /// which we treat as intentional teardown via `JoinHandle::abort`).
    ///
    /// `UnboundedSender::send` is synchronous and non-blocking — it does not yield in a way
    /// that the `select!` could cancel — which is the property that prevents this path from
    /// re-deadlocking against the controller's main loop. If the receiver is gone (consumer
    /// task exited unexpectedly, or controller shutting down) the message is logged and
    /// dropped; the consumer-task supervisor on the controller side will surface a panic via
    /// `background_task_failed`.
    fn notify_domain_exited(
        &self,
        addr: DomainIndex,
        result: &Result<ReadySetResult<()>, JoinError>,
    ) {
        log_domain_result(addr, result);
        if matches!(result, Err(e) if e.is_cancelled()) {
            return;
        }
        if let Err(error) = self.domain_exited_tx.send(addr) {
            warn!(%error, %addr, "domain-exited channel closed; not notifying controller");
        }
    }

    /// Run the worker continuously, processing worker requests, heartbeats, and domain failures.
    ///
    /// This function returns if the worker request sender is dropped.
    pub async fn run(mut self) {
        loop {
            select! {
                // We use `biased` here to ensure that our shutdown signal will be received and
                // acted upon even if the other branches in this `select!` are constantly in a
                // ready state (e.g. a stream that has many messages where very little time passes
                // between receipt of these messages). More information about this situation can
                // be found in the docs for `tokio::select`.
                biased;
                _ = self.shutdown_rx.recv() => {
                    debug!("worker shutting down after shutdown signal received");
                    let keys: Vec<_> = self.domains.keys().cloned().collect();
                    if let Ok(keys) = Vec1::try_from_vec(keys) {
                        if let Err(e) = self.kill_domains_impl(keys).await {
                            warn!(error = %e, "error on shutting down domains");
                        }
                    }
                    return;
                }
                req = self.rx.recv() => {
                    if let Some(req) = req {
                        self.process_worker_request(req).await;
                    } else {
                        debug!("worker shutting down after request handle dropped");
                        return;
                    }
                }
                _ = self.evict_interval.tick() => {
                    self.evict_tick();
                }
                Some((result, domain)) = self.domain_wait_queue.next() => {
                    self.notify_domain_exited(domain, &result);
                }
            }
        }
    }
}

async fn evict_check(
    limit: usize,
    coord: Arc<ChannelCoordinator>,
    tracker: MemoryTracker,
    state_sizes: Arc<Mutex<HashMap<DomainIndex, Arc<AtomicUsize>>>>,
    bmgr: Arc<BarrierManager>,
) -> ReadySetResult<()> {
    let start = Instant::now();

    let used = tracker.allocated_bytes()?;
    gauge!(recorded::EVICTION_WORKER_HEAP_ALLOCATED_BYTES).set(used as f64);
    if used < limit {
        return Ok(());
    }
    counter!(recorded::EVICTION_WORKER_EVICTION_RUNS).increment(1);

    // add current state sizes (could be out of date, as packet sent below is not
    // necessarily received immediately)
    let (mut sizes, total_reported) = {
        let state_sizes = state_sizes.lock().await;
        let mut total_reported = 0;
        let sizes = state_sizes
            .iter()
            .filter_map(|(domain, size_atom)| {
                let size = size_atom.load(Ordering::Acquire);
                if size == 0 {
                    return None;
                }
                trace!("domain {} state size is {} bytes", domain, size);
                total_reported += size;
                Some((*domain, size))
            })
            .collect::<Vec<_>>();
        (sizes, total_reported)
    };

    let actual_over = used - limit;

    // state sizes are under actual memory usage, but roughly proportional to actual
    // memory usage - let's figure out proportionally how much *reported* memory we
    // should evict
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
    let mut barrier = bmgr.create(n).await;

    let mut domain_senders = HashMap::new();
    for &(target, size) in sizes.iter().rev() {
        // TODO: should this be evenly divided, or weighted by the size of the domains?
        let share = proportional_over.div_ceil(n);
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

        debug!(
            "memory footprint ({} bytes) exceeds limit ({} bytes); evicting from largest domain {}",
            used, limit, target,
        );

        counter!(recorded::EVICTION_WORKER_EVICTIONS_REQUESTED).increment(1);

        let tx = match domain_senders.entry(target) {
            Occupied(entry) => entry.into_mut(),
            Vacant(entry) => entry.insert(coord.connect_to(&target)?),
        };
        let pkt = Packet::Evict(Evict {
            req: Eviction::Bytes {
                node: None,
                num_bytes: evict,
            },
            barrier: Some(BarrierCredit {
                id: barrier.id(),
                credits: barrier.split(),
            }),
        });
        if let Err(e) = tx.send(pkt).await {
            // probably exiting?
            warn!("failed to evict from {}: {}", target.index(), e);
            // remove sender so we don't try to use it again
            domain_senders.remove(&target);
        }
    }

    histogram!(recorded::EVICTION_WORKER_EVICTION_TIME).record(start.elapsed().as_micros() as f64);

    barrier.await?;
    Ok(())
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
async fn evict_start(
    limit: usize,
    coord: Arc<ChannelCoordinator>,
    tracker: MemoryTracker,
    state_sizes: Arc<Mutex<HashMap<DomainIndex, Arc<AtomicUsize>>>>,
    is_evicting: Arc<AtomicBool>,
    barriers: Arc<BarrierManager>,
) -> ReadySetResult<()> {
    counter!(recorded::EVICTION_WORKER_EVICTION_TICKS).increment(1);
    if is_evicting.swap(true, Ordering::Relaxed) {
        return Ok(()); // Already evicting, nothing to do
    }

    counter!(recorded::EVICTION_WORKER_EVICTION_CHECKS).increment(1);
    let res = evict_check(limit, coord, tracker, state_sizes, barriers)
        .instrument(info_span!("evicting"))
        .await;

    is_evicting.store(false, Ordering::Relaxed);
    res
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
                    while let Some((handle, domain)) = domain_wait_queue.next().await {
                        match handle {
                            Ok(Err(e)) => {
                                error!(domain = %domain, err = %e, "domain failed during drop")
                            }
                            Err(e) if !e.is_cancelled() => {
                                error!(domain = %domain, err = %e, "domain failed during drop")
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

struct BarrierInternal {
    credits: u128,
    done: oneshot::Sender<ReadySetResult<()>>,
}

/// Tracks in-flight barriers and accumulates returned credits.
///
/// The internal mutex is a leaf in the lock order: it is acquired by every replica's
/// select loop (returning credits via [`add`]) and by the eviction task (issuing
/// barriers via [`create`]), and no other lock is held across `.await` on it.
///
/// [`add`]: BarrierManager::add
/// [`create`]: BarrierManager::create
#[derive(Default)]
pub struct BarrierManager {
    barriers: Mutex<HashMap<u128, BarrierInternal>>,
}

impl BarrierManager {
    const FULL_CREDIT: u128 = u128::MAX;

    async fn create(&self, split: usize) -> Barrier {
        let mut barriers = self.barriers.lock().await;
        let id = loop {
            let id = rand::rng().random();
            let None = barriers.get(&id) else {
                continue;
            };
            break id;
        };

        let (tx, rx) = oneshot::channel();
        barriers.insert(
            id,
            BarrierInternal {
                credits: 0,
                done: tx,
            },
        );
        let b = Barrier::new(id, split as _, rx);
        if split == 0 {
            Self::notify(barriers, id);
        }
        b
    }

    async fn add(&self, credit: BarrierCredit) {
        counter!(recorded::BARRIER_CREDITS_RETURNED).increment(1);
        let mut barriers = self.barriers.lock().await;
        let Some(b) = barriers.get_mut(&credit.id) else {
            error!("unknown barrier {:x}", credit.id);
            return;
        };

        b.credits += credit.credits;
        if b.credits == Self::FULL_CREDIT {
            Self::notify(barriers, credit.id);
        }
    }

    fn notify(mut barriers: MutexGuard<'_, HashMap<u128, BarrierInternal>>, id: u128) {
        let b = barriers.remove(&id).unwrap(); // just looked up
        let _ = b.done.send(Ok(())).inspect_err(|e| {
            error!("receiver dropped for barrier {:x}: {:?}", id, e);
        });
    }
}

#[derive(Default)]
struct BarrierWait {
    complete: bool,
    waker: Option<Waker>,
}

impl BarrierWait {
    const BARRIER_WAIT_MAX: Duration = Duration::from_secs(10);

    fn new() -> Arc<std::sync::Mutex<Self>> {
        let new = Arc::new(Self::default().into());
        Self::start(Arc::clone(&new));
        new
    }

    fn start(wait: Arc<std::sync::Mutex<Self>>) {
        tokio::task::spawn(async move {
            tokio::time::sleep(Self::BARRIER_WAIT_MAX).await;
            let mut wait = wait.lock().unwrap();
            wait.complete = true;
            if let Some(waker) = wait.waker.take() {
                waker.wake();
            }
        });
    }
}

pub struct Barrier {
    id: u128,
    done: oneshot::Receiver<ReadySetResult<()>>,
    complete: bool,
    give: Vec<u128>,
    wait: Arc<std::sync::Mutex<BarrierWait>>,
}

impl Barrier {
    fn new(id: u128, split: u128, done: oneshot::Receiver<ReadySetResult<()>>) -> Self {
        debug!("creating barrier {:x} with {} splits", id, split);
        let mut give = Vec::new();
        if let Some(each) = BarrierManager::FULL_CREDIT.checked_div(split) {
            let extra = BarrierManager::FULL_CREDIT % split;
            assert!(each > 0, "barrier split too many times");
            give.push(each + extra);
            for _ in 1..split {
                give.push(each);
            }
        }

        Self {
            id,
            done,
            complete: false,
            give,
            wait: BarrierWait::new(),
        }
    }

    fn id(&self) -> u128 {
        self.id
    }

    fn split(&mut self) -> u128 {
        self.give
            .pop()
            .expect("barrier split more times than declared")
    }
}

impl Future for Barrier {
    type Output = ReadySetResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match pin!(&mut self.done).poll(cx) {
            Poll::Pending => {
                let mut wait = self.wait.lock().unwrap();
                if wait.complete {
                    drop(wait);
                    self.complete = true;
                    Poll::Ready(Err(internal_err!("barrier timeout")))
                } else {
                    wait.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
            Poll::Ready(Ok(res)) => {
                debug!("barrier {:x} complete", self.id);
                self.complete = true;
                Poll::Ready(res)
            }
            Poll::Ready(Err(e)) => {
                error!("barrier {:x} errored: {}", self.id, e);
                self.complete = true;
                Poll::Ready(Err(internal_err!(
                    "barrier sender unexpectedly dropped: {:?}",
                    e
                )))
            }
        }
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        if !self.complete {
            error!("barrier {:x} dropped before completion", self.id);
        }
    }
}

#[cfg(test)]
mod test {
    use futures::future::FutureExt;

    use super::*;

    #[tokio::test]
    async fn barrier_split() {
        const SPLITS: usize = 5;

        let bm = BarrierManager::default();
        let mut b = bm.create(SPLITS).await;

        for _ in 0..SPLITS {
            bm.add(BarrierCredit {
                id: b.id,
                credits: b.split(),
            })
            .await;
        }

        // b.await, but require that it resolve immediately
        assert_eq!(b.now_or_never(), Some(Ok(())));
    }

    #[tokio::test]
    async fn barrier_zero() {
        let bm = BarrierManager::default();
        let b = bm.create(0).await;
        assert_eq!(b.now_or_never(), Some(Ok(())));
    }

    #[tokio::test]
    async fn barrier_timeout() {
        let bm = BarrierManager::default();
        let b = bm.create(1).await;
        assert!(b.await.is_err());
    }
}
