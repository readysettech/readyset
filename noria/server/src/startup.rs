//! # The Noria server
//!
//! A Noria server instance consists of three main components: the `NoriaServer` HTTP server
//! (which listens externally for RPC calls), the `ControllerOuter` controller wrapper (which may
//! contain a `ControllerInner` if this instance is elected leader), and the `Worker` (which
//! contains many `Domain` objects).
//!
//! We also start the reader listening loop (`worker::readers::listen`, responsible for servicing
//! data-plane read requests) and the controller's authority manager
//! (`controller::authority`, responsible for findind leadership information via
//! the `Authority`, keeping track of workers, and winning the leadership election if possible).
//!
//! These are all spun up by the `start_instance` function in this module, and run on the Tokio
//! event loop. This gives you a `Handle`, enabling you to send requests to the `ControllerOuter`.
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
//! - registration and heartbeat requests sent from workers to the `ControllerInner`
//!   - `ControllerInner::handle_register`, mapped to `POST /worker_rx/register`
//!   - `ControllerInner::handle_heartbeat`, mapped to `POST /worker_rx/heartbeat`
//! - requests sent from the `ControllerInner` to workers (including those workers' domains)
//!   - the `WorkerRequestKind` enum, mapped to `POST /worker_request`
//!   - ...which can contain a `DomainRequest`
//! - requests sent from clients to the `ControllerInner`
//!   - see `ControllerInner::external_request`
//! - other misc. endpoints for things like metrics (see the `NoriaServer` implementation for more)
//!
//! Typically, an incoming HTTP request is deserialized and then sent via a `tokio::sync::mpsc`
//! channel to the `ControllerOuter` or `Worker`; the response is sent back via a
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

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time;
use std::time::Duration;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::future::TryFutureExt;
use hyper::{self, header::CONTENT_TYPE, Method, StatusCode};
use hyper::{service::make_service_fn, Body, Request, Response};
use stream_cancel::Valve;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;
use url::Url;

use dataflow::Readers;
use noria::consensus::Authority;
use noria::metrics::recorded;
use noria::ControllerDescriptor;
use noria::ReadySetError;

use crate::controller::{ControllerOuter, ControllerRequest};
use crate::handle::Handle;
use crate::metrics::{get_global_recorder, Clear, RecorderType};
use crate::worker::{Worker, WorkerRequest};
use crate::{Config, VolumeId};

/// Start up a new instance and return a handle to it. Dropping the handle will stop the
/// instance. Make sure that this method is run while on a runtime.
pub(super) async fn start_instance<A: Authority + 'static>(
    authority: Arc<A>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    config: Config,
    memory_limit: Option<usize>,
    _memory_check_frequency: Option<time::Duration>,
    log: slog::Logger,
    region: Option<String>,
    replicator_url: Option<String>,
    reader_only: bool,
    volume_id: Option<VolumeId>,
) -> Result<Handle<A>, anyhow::Error> {
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel(16);
    let (controller_tx, controller_rx) = tokio::sync::mpsc::channel(16);
    let (authority_tx, authority_rx) = tokio::sync::mpsc::channel(16);
    let (handle_tx, handle_rx) = tokio::sync::mpsc::channel(16);

    let (trigger, valve) = Valve::new();

    let http_listener = TcpListener::bind(SocketAddr::new(listen_addr, external_addr.port()))
        .or_else(|_| {
            warn!(
                log,
                "Could not bind to provided external port; using a random port instead!"
            );
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

    tokio::spawn(crate::worker::readers::listen(
        valve.clone(),
        readers_listener,
        readers.clone(),
    ));

    tokio::spawn(
        hyper::server::Server::builder(hyper::server::accept::from_stream(
            valve.wrap(TcpListenerStream::new(http_listener)),
        ))
        .serve(make_service_fn(move |_| {
            let s = http_server.clone();
            async move { io::Result::Ok(s) }
        }))
        .map_err(|e| {
            panic!("HTTP server failed: {}", e);
        }),
    );

    let worker = Worker {
        log: log.new(o!("module" => "worker")),
        election_state: None,
        // this initial duration doesn't matter; it gets set upon worker registration
        heartbeat_interval: tokio::time::interval(Duration::from_secs(10)),
        evict_interval: None,
        memory_limit,
        rx: worker_rx,
        coord: Arc::new(Default::default()),
        http: reqwest::Client::new(),
        worker_uri: http_uri.clone(),
        domain_bind: listen_addr,
        domain_external: external_addr.ip(),
        reader_addr,
        state_sizes: Arc::new(Mutex::new(Default::default())),
        readers,
        valve: valve.clone(),
        domains: Default::default(),
        region: region.clone(),
        reader_only,
        volume_id,
    };

    tokio::spawn(worker.run().map_err(|e| {
        panic!("Worker failed: {}", e.to_string());
    }));

    let our_descriptor = ControllerDescriptor {
        controller_uri: http_uri,
        nonce: rand::random(),
    };

    let controller = ControllerOuter {
        log: log.new(o!("module" => "controller")),
        inner: None,
        authority: authority.clone(),
        worker_tx,
        campaign_rx: authority_rx,
        http_rx: controller_rx,
        handle_rx,
        our_descriptor: our_descriptor.clone(),
        valve: valve.clone(),
        replicator_url,
        replicator_task: None,
    };

    crate::controller::authority_runner(
        authority_tx,
        authority.clone(),
        our_descriptor.clone(),
        config,
        tokio::runtime::Handle::current(),
        region,
    );

    tokio::spawn(controller.run().map_err(|e| {
        panic!("ControllerOuter failed: {}", e.to_string());
    }));

    let handle = Handle::new(authority, handle_tx, trigger, our_descriptor).await?;

    Ok(handle)
}

/// The main Noria HTTP server object.
struct NoriaServer<A> {
    /// Channel to the running `Worker`.
    worker_tx: Sender<WorkerRequest>,
    /// Channel to the running `ControllerOuter`.
    controller_tx: Sender<ControllerRequest>,
    /// The `Authority` used inside the server.
    authority: Arc<A>,
}

impl<A> Clone for NoriaServer<A> {
    fn clone(&self) -> Self {
        Self {
            worker_tx: self.worker_tx.clone(),
            controller_tx: self.controller_tx.clone(),
            authority: self.authority.clone(),
        }
    }
}

impl<A: Authority> Service<Request<Body>> for NoriaServer<A> {
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
                let res = match self.authority.try_read_raw(&format!("/{}", &path[11..])) {
                    Ok(Some(data)) => res
                        .header(CONTENT_TYPE, "application/json")
                        .body(hyper::Body::from(data)),
                    _ => res.status(StatusCode::NOT_FOUND).body(hyper::Body::empty()),
                };
                Box::pin(async move { Ok(res.unwrap()) })
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
