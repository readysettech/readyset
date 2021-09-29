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
use std::sync::{Arc, Mutex};
use std::time;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::{io, process};

use futures_util::future::{Either, TryFutureExt};
use hyper::{self, header::CONTENT_TYPE, Method, StatusCode};
use hyper::{service::make_service_fn, Body, Request, Response};
use launchpad::futures::abort_on_panic;
use stream_cancel::Valve;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;
use tracing::{error, warn};
use url::Url;

use dataflow::Readers;
use noria::consensus::{Authority, AuthorityControl};
use noria::metrics::recorded;
use noria::ReadySetError;
use noria::{ControllerDescriptor, WorkerDescriptor};

use crate::controller::{Controller, ControllerRequest};
use crate::handle::Handle;
use crate::metrics::{get_global_recorder, Clear, RecorderType};
use crate::worker::{Worker, WorkerRequest};
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
) -> Result<Handle, anyhow::Error> {
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(16);
    let (controller_tx, controller_rx) = tokio::sync::mpsc::channel(16);
    let (handle_tx, handle_rx) = tokio::sync::mpsc::channel(16);

    let (trigger, valve) = Valve::new();

    let http_listener = TcpListener::bind(SocketAddr::new(listen_addr, external_addr.port()))
        .or_else(|_| {
            warn!("Could not bind to provided external port; using a random port instead!");
            TcpListener::bind(SocketAddr::new(listen_addr, 0))
        })
        .await?;

    let real_external_addr =
        SocketAddr::new(external_addr.ip(), http_listener.local_addr()?.port());
    // FIXME(eta): this won't work for IPv6
    let http_uri = Url::parse(&format!("http://{}", real_external_addr))?;

    let readers_listener = TcpListener::bind(SocketAddr::new(listen_addr, 0)).await?;

    let reader_addr = SocketAddr::new(external_addr.ip(), readers_listener.local_addr()?.port());
    let readers: Readers = Arc::new(Mutex::new(Default::default()));

    let http_server = NoriaServer {
        worker_tx: worker_tx.clone(),
        controller_tx,
        authority: authority.clone(),
    };

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

    tokio::spawn(maybe_abort_on_panic!(crate::worker::readers::listen(
        valve.clone(),
        readers_listener,
        readers.clone(),
    )));

    {
        tokio::spawn(maybe_abort_on_panic!(hyper::server::Server::builder(
            hyper::server::accept::from_stream(valve.wrap(TcpListenerStream::new(http_listener)),)
        )
        .serve(make_service_fn(move |_| {
            let s = http_server.clone();
            async move { io::Result::Ok(s) }
        }))
        .map_err(move |e| {
            error!(error = %e, "HTTP server failed");
            if abort_on_task_failure {
                process::abort();
            }
        })));
    }

    let worker = Worker {
        election_state: None,
        // this initial duration doesn't matter; it gets set upon worker registration
        evict_interval: memory_check_frequency.map(|f| tokio::time::interval(f)),
        memory_limit,
        rx: worker_rx,
        coord: Arc::new(Default::default()),
        domain_bind: listen_addr,
        domain_external: external_addr.ip(),
        state_sizes: Arc::new(Mutex::new(Default::default())),
        readers,
        valve: valve.clone(),
        domains: Default::default(),
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

    let controller = Controller {
        inner: None,
        authority: authority.clone(),
        worker_tx,
        http_rx: controller_rx,
        handle_rx,
        our_descriptor: our_descriptor.clone(),
        valve: valve.clone(),
        worker_descriptor,
        config,
        authority_task: None,
    };

    tokio::spawn(maybe_abort_on_panic!(controller.run().map_err(move |e| {
        error!(error = %e, "Controller failed");
        if abort_on_task_failure {
            process::abort()
        }
    })));

    Ok(Handle::new(authority, handle_tx, trigger, our_descriptor))
}

/// The main Noria HTTP server object.
struct NoriaServer {
    /// Channel to the running `Worker`.
    worker_tx: Sender<WorkerRequest>,
    /// Channel to the running `Controller`.
    controller_tx: Sender<ControllerRequest>,
    /// The `Authority` used inside the server.
    authority: Arc<Authority>,
}

impl Clone for NoriaServer {
    fn clone(&self) -> Self {
        Self {
            worker_tx: self.worker_tx.clone(),
            controller_tx: self.controller_tx.clone(),
            authority: self.authority.clone(),
        }
    }
}

impl Service<Request<Body>> for NoriaServer {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let res = Response::builder()
            // disable CORS to allow use as API server
            .header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");

        metrics::increment_counter!(recorded::SERVER_EXTERNAL_REQUESTS);

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/graph.html") => {
                let res = res
                    .header(CONTENT_TYPE, "text/html")
                    .body(hyper::Body::from(include_str!("graph.html")));
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::GET, path) if path.starts_with("/zookeeper/") => {
                let zookeeper_path = path.to_owned();
                let authority = self.authority.clone();
                Box::pin(async move {
                    let res = match authority
                        .try_read_raw(&format!("/{}", &zookeeper_path["/zookeeper/".len()..]))
                        .await
                    {
                        Ok(Some(data)) => res
                            .header(CONTENT_TYPE, "application/json")
                            .body(hyper::Body::from(data)),
                        _ => res.status(StatusCode::NOT_FOUND).body(hyper::Body::empty()),
                    };
                    Ok(res.unwrap())
                })
            }
            (&Method::GET, "/prometheus") => {
                let body =
                    get_global_recorder().flush_and_then(|x| x.render(RecorderType::Prometheus));
                let res = res.header(CONTENT_TYPE, "text/plain");
                let res = match body {
                    Some(metrics) => res.body(hyper::Body::from(metrics)),
                    None => res
                        .status(404)
                        .body(hyper::Body::from("Prometheus metrics were not enabled. To fix this, run Noria with --prometheus-metrics".to_string())),
                };
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::POST, "/metrics_dump") => {
                let res = get_global_recorder().flush_and_then(|recorder| {
                    match recorder.render(RecorderType::Noria) {
                        Some(metrics) => res
                            .header(CONTENT_TYPE, "application/json")
                            .body(hyper::Body::from(metrics)),
                        None => res
                            .status(404)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(hyper::Body::from("Noria metrics were not enabled. To fix this, run Noria with --noria-metrics".to_string())),
                    }
                });
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::POST, "/reset_metrics") => {
                get_global_recorder().clear();
                let res = res
                    .header(CONTENT_TYPE, "application/json")
                    .body(hyper::Body::from(vec![]));
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::POST, "/worker_request") => {
                metrics::increment_counter!(recorded::SERVER_WORKER_REQUESTS);

                let wtx = self.worker_tx.clone();
                Box::pin(async move {
                    let body = hyper::body::to_bytes(req.into_body()).await?;
                    let wrq = match bincode::deserialize(&body) {
                        Ok(x) => x,
                        Err(e) => {
                            return Ok(res
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .header("Content-Type", "text/plain; charset=utf-8")
                                .body(hyper::Body::from(
                                    bincode::serialize(&ReadySetError::from(e)).unwrap(),
                                ))
                                .unwrap());
                        }
                    };
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    if wtx
                        .send(WorkerRequest {
                            kind: wrq,
                            done_tx: tx,
                        })
                        .await
                        .is_err()
                    {
                        let res = res.status(StatusCode::SERVICE_UNAVAILABLE);
                        return Ok(res.body(hyper::Body::empty()).unwrap());
                    }

                    let res = match rx.await {
                        Ok(Ok(ret)) => res.header("Content-Type", "application/octet-stream").body(
                            hyper::Body::from(
                                ret.unwrap_or_else(|| bincode::serialize(&()).unwrap()),
                            ),
                        ),
                        Ok(Err(e)) => res
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "application/octet-stream")
                            .body(hyper::Body::from(bincode::serialize(&e).unwrap())),
                        Err(_) => res
                            .status(StatusCode::SERVICE_UNAVAILABLE)
                            .body(hyper::Body::empty()),
                    };
                    Ok(res.unwrap())
                })
            }
            _ => {
                metrics::increment_counter!(recorded::SERVER_CONTROLLER_REQUESTS);

                let method = req.method().clone();
                let path = req.uri().path().to_string();
                let query = req.uri().query().map(ToOwned::to_owned);
                let controller_tx = self.controller_tx.clone();

                Box::pin(async move {
                    let body = hyper::body::to_bytes(req.into_body()).await?;
                    let (tx, rx) = tokio::sync::oneshot::channel();

                    let req = ControllerRequest {
                        method,
                        path,
                        query,
                        body,
                        reply_tx: tx,
                    };

                    if controller_tx.send(req).await.is_err() {
                        let res = res
                            .status(StatusCode::SERVICE_UNAVAILABLE)
                            .header("Content-Type", "text/plain; charset=utf-8");
                        return Ok(res.body(hyper::Body::from("server went away")).unwrap());
                    }

                    match rx.await {
                        Ok(reply) => {
                            let res = match reply {
                                Ok(Ok(reply)) => res
                                    .header("Content-Type", "application/octet-stream")
                                    .body(hyper::Body::from(reply)),
                                Ok(Err(reply)) => res
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .header("Content-Type", "application/octet-stream")
                                    .body(hyper::Body::from(reply)),
                                Err(status_code) => {
                                    res.status(status_code).body(hyper::Body::empty())
                                }
                            };
                            Ok(res.unwrap())
                        }
                        Err(_) => {
                            let res = res.status(StatusCode::NOT_FOUND);
                            Ok(res.body(hyper::Body::empty()).unwrap())
                        }
                    }
                })
            }
        }
    }
}
