use crate::coordination::{DomainDescriptor, RunDomainResponse};
use crate::errors::internal_err;
use crate::worker::replica::WrappedDomainRequest;
use crate::ReadySetResult;
use dataflow::{DomainBuilder, DomainRequest, Packet, Readers};
use futures_util::{future::TryFutureExt, sink::SinkExt, stream::StreamExt};
use launchpad::select;
use metrics::{counter, gauge, histogram};
use noria::internal::DomainIndex;
use noria::metrics::recorded;
use noria::{channel, ReadySetError};
use replica::ReplicaAddr;
use serde::{Deserialize, Serialize};
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
use stream_cancel::Valve;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Interval;
use tracing::{debug, error, info, info_span, trace, warn};
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
}

impl Worker {
    fn process_eviction(&mut self) {
        tokio::spawn(do_eviction(
            self.memory_limit,
            self.coord.clone(),
            self.state_sizes.clone(),
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
                self.domains.clear();
                for (_, d) in self.domains.drain() {
                    d.join_handle.abort();
                }
                self.election_state = Some(WorkerElectionState { controller_uri });
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
                        info!("worker shutting down after request handle dropped");
                        return;
                    }
                }
                failed_domains = poll_domains(&mut self.domains) => {
                    for (idx, shard, err) in failed_domains {
                        match err {
                            Err(e) => error!(domain_index = idx.index(), shard, error = %e, "domain failed"),
                            Ok(Err(e)) => error!(domain_index = idx.index(), shard, error = %e, "domain failed"),
                            Ok(Ok(())) => error!(domain_index = idx.index(), shard, "domain exited unexpectedly"),
                        }
                        self.domains.remove(&(idx, shard));
                    }
                }
                _ = shutdown_stream.next() => {
                    info!("worker shutting down after valve shut");
                    return;
                }
                _ = eviction => {
                    self.process_eviction();
                }
            }
        }
    }
}

#[allow(clippy::type_complexity)]
async fn do_eviction(
    memory_limit: Option<usize>,
    coord: Arc<ChannelCoordinator>,
    state_sizes: Arc<Mutex<HashMap<(DomainIndex, usize), Arc<AtomicUsize>>>>,
) -> ReadySetResult<()> {
    let span = info_span!("evicting");
    let mut domain_senders = HashMap::new();

    use std::cmp;

    let start = std::time::Instant::now();

    // 2. add current state sizes (could be out of date, as packet sent below is not
    //    necessarily received immediately)
    let mut sizes: Vec<((DomainIndex, usize), usize)> = tokio::task::block_in_place(|| {
        let state_sizes = state_sizes.lock().unwrap();
        state_sizes
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

                    span.in_scope(|| {
                        debug!(
                            "memory footprint ({} bytes) exceeds limit ({} bytes); evicting from largest domain {}",
                            total,
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
