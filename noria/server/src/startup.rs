//! # The Noria server
//!
//! A Noria server instance consists of three main components: the `NoriaServer` HTTP server
//! (which listens externally for RPC calls), the `Controller` controller wrapper (which may
//! contain a `Leader` if this instance is elected leader), and the `Worker` (which
//! contains many `Domain` objects).
//!
//! We also start the reader listening loop (`worker::readers::listen`, responsible for servicing
//! data-plane read requests) and the controller's authority manager
//! (`controller::authority`, responsible for findind leadership information via
//! the `Authority`, keeping track of workers, and winning the leadership election if possible).
//!
//! These are all spun up by the `start_instance` function in this module, and run on the Tokio
//! event loop. This gives you a `Handle`, enabling you to send requests to the `Controller`.
//!
//! # Control plane and data plane
//!
//! Communications amongst and within Noria servers are divided into 2 categories:
//!
//! - the **control plane**, communications between workers and controllers used to perform
//!   migrations and alter the structure of the system
//! - the **data plane**, communications relating to client-initiated read and write traffic, and
//!   dataflow communications between domains
//!
//! It's important to note that data plane communications are very much "in the hot path", since
//! delays and slowdowns directly impact user-observed read/write performance.
//!
//! ## Control plane communications overview
//!
//! All control plane communications go via server instances' `NoriaServer`s, which are just HTTP
//! servers. The endpoints exposed by this HTTP server are:
//!
//! - requests sent from the `Leader` to workers (including those workers' domains)
//!   - the `WorkerRequestKind` enum, mapped to `POST /worker_request`
//!   - ...which can contain a `DomainRequest`
//! - requests sent from clients to the `Leader`
//!   - see `Leader::external_request`
//! - other misc. endpoints for things like metrics (see the `NoriaServer` implementation for more)
//!
//! Typically, an incoming HTTP request is deserialized and then sent via a `tokio::sync::mpsc`
//! channel to the `Controller` or `Worker`; the response is sent back via a
//! `tokio::sync::oneshot` channel, serialized, and returned to the client.
//!
//! ## Data plane communications overview
//!
//! Data plane communications occur over TCP sockets, and use custom protocols optimized for speed.
//!
//! (TODO: write this section)

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::{process, time};

use dataflow::Readers;
use futures_util::future::{Either, TryFutureExt};
use launchpad::futures::abort_on_panic;
use noria::consensus::Authority;
use noria::{ControllerDescriptor, WorkerDescriptor};
use stream_cancel::{Trigger, Valve};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;
use url::Url;

use crate::controller::{Controller, ControllerRequest, HandleRequest};
use crate::handle::Handle;
use crate::http_router::NoriaServerHttpRouter;
use crate::worker::{MemoryTracker, Worker, WorkerRequest};
use crate::{Config, VolumeId};

macro_rules! maybe_abort_on_panic {
    ($abort_on_task_failure: expr, $fut: expr) => {{
        let fut = $fut;
        if $abort_on_task_failure {
            Either::Left(abort_on_panic(fut))
        } else {
            Either::Right(fut)
        }
    }};
}

/// Starts the reader layer of the noria server instance. Returns the external address
/// of the reader listener.
async fn start_readers(
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    upquery_timeout: time::Duration,
    abort_on_task_failure: bool,
    readers: Readers,
    valve: Valve,
) -> Result<SocketAddr, anyhow::Error> {
    let readers_listener = TcpListener::bind(SocketAddr::new(listen_addr, 0)).await?;
    let reader_addr = SocketAddr::new(external_addr.ip(), readers_listener.local_addr()?.port());
    tokio::spawn(maybe_abort_on_panic!(
        abort_on_task_failure,
        crate::worker::readers::listen(
            valve.clone(),
            readers_listener,
            readers.clone(),
            upquery_timeout,
        )
    ));

    Ok(reader_addr)
}

async fn start_worker(
    worker_rx: Receiver<WorkerRequest>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    abort_on_task_failure: bool,
    readers: Readers,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    valve: Valve,
) -> Result<(), anyhow::Error> {
    let worker = Worker {
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
        valve,
        domains: Default::default(),
        memory: MemoryTracker::new()?,
        is_evicting: Default::default(),
        domain_wait_queue: Default::default(),
    };

    tokio::spawn(maybe_abort_on_panic!(abort_on_task_failure, worker.run()));
    Ok(())
}

async fn start_controller(
    authority: Arc<Authority>,
    http_uri: Url,
    reader_addr: SocketAddr,
    config: Config,
    worker_tx: Sender<WorkerRequest>,
    handle_rx: Receiver<HandleRequest>,
    controller_rx: Receiver<ControllerRequest>,
    region: Option<String>,
    abort_on_task_failure: bool,
    reader_only: bool,
    volume_id: Option<VolumeId>,
    valve: Valve,
) -> Result<ControllerDescriptor, anyhow::Error> {
    let our_descriptor = ControllerDescriptor {
        controller_uri: http_uri.clone(),
        nonce: rand::random(),
    };

    let worker_descriptor = WorkerDescriptor {
        worker_uri: http_uri,
        reader_addr,
        region,
        reader_only,
        volume_id,
    };

    let controller = Controller::new(
        authority,
        worker_tx,
        controller_rx,
        handle_rx,
        our_descriptor.clone(),
        valve,
        worker_descriptor,
        config,
    );

    tokio::spawn(maybe_abort_on_panic!(
        abort_on_task_failure,
        controller.run().map_err(move |e| {
            error!(error = %e, "Controller failed");
            if abort_on_task_failure {
                process::abort()
            }
        })
    ));

    Ok(our_descriptor)
}

async fn start_request_router(
    authority: Arc<Authority>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    worker_tx: Sender<WorkerRequest>,
    controller_tx: Sender<ControllerRequest>,
    abort_on_task_failure: bool,
    valve: Valve,
) -> Result<Url, anyhow::Error> {
    let http_server = NoriaServerHttpRouter {
        listen_addr,
        port: external_addr.port(),
        valve,
        worker_tx: worker_tx.clone(),
        controller_tx,
        authority: authority.clone(),
    };

    let http_listener = http_server.create_listener().await?;
    let real_external_addr =
        SocketAddr::new(external_addr.ip(), http_listener.local_addr()?.port());
    // FIXME(eta): this won't work for IPv6
    let http_uri = Url::parse(&format!("http://{}", real_external_addr))?;
    tokio::spawn(maybe_abort_on_panic!(
        abort_on_task_failure,
        NoriaServerHttpRouter::route_requests(http_server, http_listener)
    ));

    Ok(http_uri)
}

/// Creates the instance, with an existing set of readers and a valve to cancel the intsance.
pub async fn start_instance_inner(
    authority: Arc<Authority>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    region: Option<String>,
    reader_only: bool,
    volume_id: Option<VolumeId>,
    readers: Readers,
    reader_addr: SocketAddr,
    valve: Valve,
    trigger: Trigger,
) -> Result<Handle, anyhow::Error> {
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(16);
    let (controller_tx, controller_rx) = tokio::sync::mpsc::channel(16);
    let (handle_tx, handle_rx) = tokio::sync::mpsc::channel(16);

    let Config {
        abort_on_task_failure,
        ..
    } = config;

    let http_uri = start_request_router(
        authority.clone(),
        listen_addr,
        external_addr,
        worker_tx.clone(),
        controller_tx,
        abort_on_task_failure,
        valve.clone(),
    )
    .await?;

    start_worker(
        worker_rx,
        listen_addr,
        external_addr,
        abort_on_task_failure,
        readers,
        memory_limit,
        memory_check_frequency,
        valve.clone(),
    )
    .await?;

    let our_descriptor = start_controller(
        authority.clone(),
        http_uri,
        reader_addr,
        config,
        worker_tx,
        handle_rx,
        controller_rx,
        region,
        abort_on_task_failure,
        reader_only,
        volume_id,
        valve,
    )
    .await?;

    Ok(Handle::new(authority, handle_tx, trigger, our_descriptor))
}

/// Start up a new instance and return a handle to it. Dropping the handle will stop the
/// instance. Make sure that this method is run while on a runtime.
pub(super) async fn start_instance(
    authority: Arc<Authority>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    region: Option<String>,
    reader_only: bool,
    volume_id: Option<VolumeId>,
) -> Result<Handle, anyhow::Error> {
    let (trigger, valve) = Valve::new();
    let Config {
        abort_on_task_failure,
        upquery_timeout,
        ..
    } = config;

    let readers: Readers = Arc::new(Mutex::new(Default::default()));
    let reader_addr = start_readers(
        listen_addr,
        external_addr,
        upquery_timeout,
        abort_on_task_failure,
        readers.clone(),
        valve.clone(),
    )
    .await?;

    start_instance_inner(
        authority,
        listen_addr,
        external_addr,
        config,
        memory_limit,
        memory_check_frequency,
        region,
        reader_only,
        volume_id,
        readers,
        reader_addr,
        valve,
        trigger,
    )
    .await
}
