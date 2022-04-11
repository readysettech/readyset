use std::cmp;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use dataflow::{DomainBuilder, DomainRequest, Packet, Readers};
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures_util::future::TryFutureExt;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use launchpad::select;
use metrics::{counter, gauge, histogram};
use noria::internal::DomainIndex;
use noria::metrics::recorded;
use noria::{channel, ReadySetError};
use noria_errors::internal_err;
use replica::ReplicaAddr;
use serde::{Deserialize, Serialize};
use stream_cancel::Valve;
use tikv_jemalloc_ctl::stats::allocated_mib;
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinError;
use tokio::time::Interval;
use tracing::{debug, error, info, info_span, trace, warn};
use url::Url;

use self::replica::Replica;
use crate::coordination::{DomainDescriptor, RunDomainResponse};
use crate::worker::replica::WrappedDomainRequest;
use crate::ReadySetResult;

/// Request handlers and utilities for reading from the ReadHandle of a
/// left-right map associated with a reader node.
pub mod readers;
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
    },

    /// A new domain should be started on this worker.
    RunDomain(DomainBuilder),

    /// Clear domains.
    ClearDomains,

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
    #[allow(dead_code)] // never read - do we care?
    controller_uri: Url,
}

/// A handle for sending messages to a domain in-process.
pub struct DomainHandle {
    req_tx: Sender<WrappedDomainRequest>,
    /// Can be used to send an abort signal to the domain
    /// aborts automatically when dropped
    _domain_abort: oneshot::Sender<()>,
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

/// A helper type which is just a map of a JoinHandle, but naming it with no dyn was too hard
pub(crate) type FinishedDomainFuture = Box<
    dyn Future<
            Output = (
                Result<Result<(), anyhow::Error>, JoinError>,
                DomainIndex,
                usize,
            ),
        > + Unpin
        + Send,
>;

/// Domains failures typically indicate that the system has entered an unrecoverable
/// state. When these situations occur, panic, to kill the worker and in production abort the
/// process. The future may be cancelled or gracefully complete when torndown, in these cases
/// do not panic.
fn handle_domain_future_completion(
    result: (
        Result<Result<(), anyhow::Error>, JoinError>,
        DomainIndex,
        usize,
    ),
) {
    let (handle, idx, shard) = result;
    match handle {
        Ok(Ok(())) => {
            warn!(
                index = idx.index(),
                shard, "domain future completed without error"
            )
        }
        Ok(Err(e)) => {
            error!(index = idx.index(), shard, err = %e, "domain failed with an error");
            panic!("domain failed: {}", e);
        }
        Err(e) if e.is_cancelled() => {
            warn!(index = idx.index(), shard, err = %e, "domain future cancelled")
        }
        Err(e) => {
            error!(index = idx.index(), shard, err = %e, "domain future failed");
            panic!("domain future failure: {}", e);
        }
    }
}

/// A Noria worker, responsible for executing some domains.
pub struct Worker {
    /// The current election state, if it exists (see the `WorkerElectionState` docs).
    pub(crate) election_state: Option<WorkerElectionState>,
    /// A timer for doing evictions.
    pub(crate) evict_interval: Option<Interval>,
    /// A memory limit for state, in bytes.
    pub(crate) memory_limit: Option<usize>,
    /// Channel through which worker requests are received.
    pub(crate) rx: Receiver<WorkerRequest>,
    /// Channel coordinator (used by domains to figure out where other domains are).
    pub(crate) coord: Arc<ChannelCoordinator>,
    /// The IP address to bind on for domain<->domain traffic.
    pub(crate) domain_bind: IpAddr,
    /// The IP address to expose to other domains for domain<->domain traffic.
    pub(crate) domain_external: IpAddr,
    /// A store of the current state size of each domain, used for eviction purposes.
    pub(crate) state_sizes: Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
    /// Read handles.
    pub(crate) readers: Readers,
    /// Valve for shutting down; triggered by the [`Handle`] when [`Handle::shutdown`] is called.
    pub(crate) valve: Valve,

    /// Handles to domains currently being run by this worker.
    ///
    /// These are indexed by (domain index, shard).
    pub(crate) domains: HashMap<(DomainIndex, usize), DomainHandle>,

    pub(crate) memory: MemoryTracker,
    pub(crate) is_evicting: Arc<AtomicBool>,
    pub(crate) domain_wait_queue: FuturesUnordered<FinishedDomainFuture>,
}

impl Worker {
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
                self.election_state = Some(WorkerElectionState { controller_uri });
                Ok(None)
            }
            WorkerRequestKind::ClearDomains => {
                info!("controller requested that this worker clears its existing domains");
                self.coord.clear();
                self.domains.clear();
                while let Some(res) = self.domain_wait_queue.next().await {
                    handle_domain_future_completion(res);
                }

                Ok(None)
            }
            WorkerRequestKind::RunDomain(builder) => {
                let idx = builder.index;
                let shard = builder.shard.unwrap_or(0);
                let span = info_span!("domain", domain_index = idx.index(), shard);
                span.in_scope(|| info!("received domain to run"));

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
                let domain =
                    builder.build(self.readers.clone(), self.coord.clone(), state_size.clone());

                // this channel is used for in-process domain traffic, to avoid going through the
                // network stack unnecessarily
                let (local_tx, local_rx) = tokio::sync::mpsc::unbounded_channel();
                // this channel is used for domain requests; it has a buffer size of 1 to prevent
                // flooding a domain with requests
                let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);

                // need to register the domain with the local channel coordinator.
                // local first to ensure that we don't unnecessarily give away remote for a
                // local thing if there's a race
                self.coord.insert_local((idx, shard), local_tx)?;
                self.coord.insert_remote((idx, shard), bind_external)?;

                self.state_sizes
                    .lock()
                    .await
                    .insert((idx, shard), state_size);

                let replica = Replica::new(domain, listener, local_rx, req_rx, self.coord.clone());
                // Each domain is single threaded in nature, so we spawn each one in a separate
                // thread, so we can avoid running blocking operations on the multi
                // threaded tokio runtime
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .max_blocking_threads(1)
                    .build()
                    .unwrap();

                let jh = Box::new(runtime.spawn(replica.run()).map(move |jh| (jh, idx, shard)));

                let (_domain_abort, domain_abort_rx) = oneshot::channel::<()>();
                // Spawn the actual thread to run the domain
                std::thread::Builder::new()
                    .name(format!("Domain {}.{}", idx, shard))
                    .stack_size(2 * 1024 * 1024) // Use the same value tokio is using
                    .spawn(move || {
                        // The runtime will run until the abort signal is sent.
                        // This will happen either if the DomainHandle is dropped (and error is
                        // recieved) or an actual signal is sent on the
                        // channel
                        let _ = runtime.block_on(domain_abort_rx);
                        runtime.shutdown_background();
                    })?;

                self.domains.insert(
                    (idx, shard),
                    DomainHandle {
                        req_tx,
                        _domain_abort,
                    },
                );

                self.domain_wait_queue.push(jh);

                span.in_scope(|| info!(%bind_actual, %bind_external, "domain booted",));
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
                    trace!(domain_index = domain.index(), shard, ?addr, "found domain",);
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
                        info!("worker shutting down after request handle dropped");
                        return;
                    }
                }
                _ = shutdown_stream.next() => {
                    info!("worker shutting down after valve shut");
                    return;
                }
                _ = eviction => {
                    self.process_eviction();
                }
                Some(res) = self.domain_wait_queue.next() => {
                    handle_domain_future_completion(res);
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
    state_sizes: Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
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

    let used: usize = memory_tracker.allocated_bytes()?;
    gauge!(recorded::EVICTION_WORKER_HEAP_ALLOCATED_BYTES, used as f64);
    // Are we over the limit?
    match memory_limit {
        None => Ok(()),
        Some(limit) => {
            if used >= limit {
                // we are! time to evict.
                // add current state sizes (could be out of date, as packet sent below is not
                // necessarily received immediately)
                let (mut sizes, total_reported) = {
                    let state_sizes = state_sizes.lock().await;
                    let mut total_reported = 0;
                    let sizes = state_sizes
                        .iter()
                        .map(|(ds, sa)| {
                            let size = sa.load(Ordering::Acquire);
                            span.in_scope(|| {
                                trace!(
                                    "domain {}.{} state size is {} bytes",
                                    ds.0.index(),
                                    ds.1,
                                    size
                                )
                            });
                            total_reported += size;
                            (*ds, size)
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
                            target.0.index(),
                        )
                    });

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
                        span.in_scope(|| warn!("failed to evict from {}: {}", target.0.index(), e));
                        // remove sender so we don't try to use it again
                        domain_senders.remove(&target);
                    }
                }
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
    /// This is only implemented for the sake of RockDB that doesn't really
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
        std::thread::spawn(move || {
            rt.block_on(async move {
                while let Some((handle, idx, shard)) = domain_wait_queue.next().await {
                    match handle {
                        Ok(Err(e)) => error!(index = idx.index(), shard, err = %e, "domain failed during drop"),
                        Err(e) if !e.is_cancelled() => error!(index = idx.index(), shard, err = %e, "domain failed during drop"),
                        _ => {}
                    }
                }
            });
        })
        .join().expect("This thread shouldn't panic");
    }
}
