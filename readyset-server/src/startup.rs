//! # The ReadySet server
//!
//! A ReadySet server instance consists of three main components: the `NoriaServer` HTTP server
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
//! Communications amongst and within ReadySet servers are divided into 2 categories:
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
use std::time::Duration;
use std::{process, time};

use dataflow::Readers;
use failpoint_macros::set_failpoint;
use futures_util::future::{Either, TryFutureExt};
use health_reporter::{HealthReporter, State as ServerState};
use readyset_alloc_metrics::report_allocator_metrics;
use readyset_client::consensus::{Authority, WorkerSchedulingConfig};
use readyset_client::{ControllerDescriptor, WorkerDescriptor};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::futures::abort_on_panic;
use readyset_util::shutdown::{self, ShutdownReceiver, ShutdownSender};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;
use url::Url;

use crate::controller::{Controller, ControllerRequest, HandleRequest};
use crate::handle::Handle;
use crate::http_router::NoriaServerHttpRouter;
use crate::worker::{Worker, WorkerRequest};
use crate::Config;

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
    shutdown_rx: ShutdownReceiver,
) -> Result<SocketAddr, anyhow::Error> {
    let readers_listener = TcpListener::bind(SocketAddr::new(listen_addr, 0)).await?;
    let reader_addr = SocketAddr::new(external_addr.ip(), readers_listener.local_addr()?.port());
    tokio::spawn(maybe_abort_on_panic!(
        abort_on_task_failure,
        crate::worker::readers::listen(
            readers_listener,
            readers.clone(),
            upquery_timeout,
            shutdown_rx,
        )
    ));

    Ok(reader_addr)
}

fn start_worker(
    worker_rx: Receiver<WorkerRequest>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    url: Url,
    abort_on_task_failure: bool,
    readers: Readers,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<Duration>,
    shutdown_rx: ShutdownReceiver,
) -> Result<(), anyhow::Error> {
    set_failpoint!(failpoints::START_WORKER);
    let worker = Worker::new(
        worker_rx,
        listen_addr,
        external_addr,
        url.join("worker_request")?,
        readers,
        memory_limit,
        memory_check_frequency,
        shutdown_rx,
    )?;

    tokio::spawn(maybe_abort_on_panic!(abort_on_task_failure, worker.run()));
    Ok(())
}

fn start_controller(
    authority: Arc<Authority>,
    http_uri: Url,
    reader_addr: SocketAddr,
    config: Config,
    worker_tx: Sender<WorkerRequest>,
    handle_rx: Receiver<HandleRequest>,
    controller_rx: Receiver<ControllerRequest>,
    abort_on_task_failure: bool,
    domain_scheduling_config: WorkerSchedulingConfig,
    leader_eligible: bool,
    telemetry_sender: TelemetrySender,
    shutdown_rx: ShutdownReceiver,
) -> Result<ControllerDescriptor, anyhow::Error> {
    set_failpoint!(failpoints::START_CONTROLLER);
    let our_descriptor = ControllerDescriptor {
        controller_uri: http_uri.clone(),
        nonce: rand::random(),
    };

    let worker_descriptor = WorkerDescriptor {
        worker_uri: http_uri,
        reader_addr,
        domain_scheduling_config,
        leader_eligible,
    };

    let controller = Controller::new(
        authority,
        worker_tx,
        controller_rx,
        handle_rx,
        our_descriptor.clone(),
        worker_descriptor,
        telemetry_sender,
        config,
        shutdown_rx,
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
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    worker_tx: Sender<WorkerRequest>,
    controller_tx: Sender<ControllerRequest>,
    abort_on_task_failure: bool,
    health_reporter: HealthReporter,
    failpoint_channel: Option<Arc<Sender<()>>>,
    shutdown_rx: ShutdownReceiver,
) -> Result<Url, anyhow::Error> {
    let http_server = NoriaServerHttpRouter {
        listen_addr,
        port: external_addr.port(),
        worker_tx: worker_tx.clone(),
        controller_tx,
        health_reporter: health_reporter.clone(),
        failpoint_channel,
    };

    let http_listener = http_server.create_listener().await?;
    let real_external_addr =
        SocketAddr::new(external_addr.ip(), http_listener.local_addr()?.port());
    // FIXME(eta): this won't work for IPv6
    let http_uri = Url::parse(&format!("http://{}", real_external_addr))?;
    tokio::spawn(maybe_abort_on_panic!(
        abort_on_task_failure,
        NoriaServerHttpRouter::route_requests(http_server, http_listener, shutdown_rx)
    ));

    Ok(http_uri)
}

#[cfg(feature = "failure_injection")]
fn maybe_create_failpoint_chann(
    wait_for_failpoint: bool,
) -> (Option<Arc<Sender<()>>>, Option<Receiver<()>>) {
    if wait_for_failpoint {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        (Some(Arc::new(tx)), Some(rx))
    } else {
        (None, None)
    }
}

#[cfg(not(feature = "failure_injection"))]
fn maybe_create_failpoint_chann(_: bool) -> (Option<Arc<Sender<()>>>, Option<Receiver<()>>) {
    (None, None)
}

#[cfg(feature = "failure_injection")]
async fn maybe_wait_for_failpoint(mut rx: Option<Receiver<()>>) {
    if let Some(mut rx) = rx.take() {
        let _ = rx.recv().await;
    }
}

#[cfg(not(feature = "failure_injection"))]
async fn maybe_wait_for_failpoint(_: Option<Receiver<()>>) {}

/// Creates the instance, with an existing set of readers and a signal sender to shut down the
/// instance.
pub(crate) async fn start_instance_inner(
    authority: Arc<Authority>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    domain_scheduling_config: WorkerSchedulingConfig,
    leader_eligible: bool,
    readers: Readers,
    reader_addr: SocketAddr,
    telemetry_sender: TelemetrySender,
    wait_for_failpoint: bool,
    shutdown_rx: ShutdownReceiver,
) -> Result<Handle, anyhow::Error> {
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(16);
    let (controller_tx, controller_rx) = tokio::sync::mpsc::channel(16);
    let (handle_tx, handle_rx) = tokio::sync::mpsc::channel(16);

    let Config {
        abort_on_task_failure,
        ..
    } = config;

    let alloc_shutdown = shutdown_rx.clone();
    tokio::spawn(report_allocator_metrics(alloc_shutdown));

    let (tx, rx) = maybe_create_failpoint_chann(wait_for_failpoint);
    let mut health_reporter = HealthReporter::new();
    let http_uri = start_request_router(
        listen_addr,
        external_addr,
        worker_tx.clone(),
        controller_tx,
        abort_on_task_failure,
        health_reporter.clone(),
        tx,
        shutdown_rx.clone(),
    )
    .await?;

    // If we previously setup a failpoint channel because wait_for_failpoint was enabled,
    // then we should wait to hear from the http router that a failpoint request was
    // handled.
    maybe_wait_for_failpoint(rx).await;

    start_worker(
        worker_rx,
        listen_addr,
        external_addr,
        http_uri.clone(),
        abort_on_task_failure,
        readers,
        memory_limit,
        memory_check_frequency,
        shutdown_rx.clone(),
    )?;

    let our_descriptor = start_controller(
        authority.clone(),
        http_uri,
        reader_addr,
        config,
        worker_tx,
        handle_rx,
        controller_rx,
        abort_on_task_failure,
        domain_scheduling_config,
        leader_eligible,
        telemetry_sender.clone(),
        shutdown_rx,
    )?;

    let _ = telemetry_sender.send_event_with_payload(
        TelemetryEvent::ServerStart,
        TelemetryBuilder::new()
            .server_version(option_env!("CARGO_PKG_VERSION").unwrap_or_default())
            .build(),
    );

    health_reporter.set_state(ServerState::Healthy);

    Ok(Handle::new(authority, handle_tx, our_descriptor))
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
    domain_scheduling_config: WorkerSchedulingConfig,
    leader_eligible: bool,
    telemetry_sender: TelemetrySender,
    wait_for_failpoint: bool,
) -> Result<(Handle, ShutdownSender), anyhow::Error> {
    let Config {
        abort_on_task_failure,
        upquery_timeout,
        ..
    } = config;

    let (shutdown_tx, shutdown_rx) = shutdown::channel();

    let readers: Readers = Arc::new(Mutex::new(Default::default()));
    let reader_addr = start_readers(
        listen_addr,
        external_addr,
        upquery_timeout,
        abort_on_task_failure,
        readers.clone(),
        shutdown_rx.clone(),
    )
    .await?;

    let controller = start_instance_inner(
        authority,
        listen_addr,
        external_addr,
        config,
        memory_limit,
        memory_check_frequency,
        domain_scheduling_config,
        leader_eligible,
        readers,
        reader_addr,
        telemetry_sender,
        wait_for_failpoint,
        shutdown_rx,
    )
    .await?;

    Ok((controller, shutdown_tx))
}
