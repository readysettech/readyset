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
//! The `do_noria_rpc` function in `noria::util` shows how clients can make RPC requests (as well
//! as the `ControllerHandle` in `noria::controller`).
//!
//! ## Data plane communications overview
//!
//! Data plane communications occur over TCP sockets, and use custom protocols optimized for speed.
//!
//! (TODO: write this section)

use std::net::{IpAddr, SocketAddr};
use std::process;
use std::sync::{Arc, Mutex};
use std::time;

use futures_util::future::{Either, TryFutureExt};
use launchpad::futures::abort_on_panic;
use stream_cancel::Valve;
use tokio::net::TcpListener;
use tracing::error;
use url::Url;

use dataflow::Readers;
use noria::consensus::Authority;
use noria::{ControllerDescriptor, WorkerDescriptor};

use crate::controller::Controller;
use crate::handle::Handle;
use crate::http_router::NoriaServerHttpRouter;
use crate::worker::{MemoryTracker, Worker};
use crate::{Config, VolumeId};

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
    should_reset_state: bool,
) -> Result<Handle, anyhow::Error> {
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(16);
    let (controller_tx, controller_rx) = tokio::sync::mpsc::channel(16);
    let (handle_tx, handle_rx) = tokio::sync::mpsc::channel(16);
    let (trigger, valve) = Valve::new();

    let readers_listener = TcpListener::bind(SocketAddr::new(listen_addr, 0)).await?;

    let reader_addr = SocketAddr::new(external_addr.ip(), readers_listener.local_addr()?.port());
    let readers: Readers = Arc::new(Mutex::new(Default::default()));

    let Config {
        abort_on_task_failure,
        ..
    } = config;

    macro_rules! maybe_abort_on_panic {
        ($fut: expr) => {{
            let fut = $fut;
            if abort_on_task_failure {
                Either::Left(abort_on_panic(fut))
            } else {
                Either::Right(fut)
            }
        }};
    }

    let http_server = NoriaServerHttpRouter {
        listen_addr,
        port: external_addr.port(),
        valve: valve.clone(),
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
        NoriaServerHttpRouter::route_requests(http_server, http_listener)
    ));

    tokio::spawn(maybe_abort_on_panic!(crate::worker::readers::listen(
        valve.clone(),
        readers_listener,
        readers.clone(),
    )));

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
        valve: valve.clone(),
        domains: Default::default(),
        memory: MemoryTracker::new()?,
        is_evicting: Default::default(),
        domain_wait_queue: Default::default(),
    };

    tokio::spawn(maybe_abort_on_panic!(worker.run()));

    let our_descriptor = ControllerDescriptor {
        controller_uri: http_uri.clone(),
        nonce: rand::random(),
    };

    let worker_descriptor = WorkerDescriptor {
        worker_uri: http_uri,
        reader_addr,
        region: region.clone(),
        reader_only,
        volume_id,
    };

    let controller = Controller::new(
        authority.clone(),
        worker_tx,
        controller_rx,
        handle_rx,
        our_descriptor.clone(),
        valve.clone(),
        worker_descriptor,
        config,
        should_reset_state,
    );

    tokio::spawn(maybe_abort_on_panic!(controller.run().map_err(move |e| {
        error!(error = %e, "Controller failed");
        if abort_on_task_failure {
            process::abort()
        }
    })));

    Ok(Handle::new(authority, handle_tx, trigger, our_descriptor))
}
