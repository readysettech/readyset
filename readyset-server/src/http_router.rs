use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::TryFutureExt;
use health_reporter::{HealthReporter, State};
use hyper::header::{CACHE_CONTROL, CONTENT_TYPE};
use hyper::service::make_service_fn;
use hyper::{self, Body, Method, Request, Response, StatusCode};
use readyset_alloc::{
    activate_prof, deactivate_prof, dump_prof_to_string, dump_stats,
    print_memory_and_per_thread_stats,
};
use readyset_client::events::ControllerEvent;
use readyset_client::metrics::recorded;
use readyset_errors::ReadySetError;
use readyset_util::shutdown::ShutdownReceiver;
use schema_catalog::SchemaCatalogUpdate;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::StreamExt as _;
use tower::Service;
use tracing::{info, warn};

use crate::controller::events::EventsHandle;
use crate::controller::ControllerRequest;
use crate::metrics::{get_global_recorder, Render};
use crate::worker::WorkerRequest;

/// Routes requests from an HTTP server to noria server workers and controllers.
/// The NoriaServerHttpRouter takes several channels (`worker_tx`, `controller_tx`)
/// used to pass messages from this context to the worker and controller threads.
/// To see the supported http requests and their respective routing, see
/// impl Service<Request<Body>> for NoriaServerHttpRouter.
#[derive(Clone)]
pub struct NoriaServerHttpRouter {
    /// The address to attempt to listen on.
    pub listen_addr: IpAddr,
    /// The port to attempt to listen on.
    pub port: u16,
    /// Channel to the running `Worker`.
    pub worker_tx: Sender<WorkerRequest>,
    /// Channel to the running `Controller`.
    pub controller_tx: Sender<ControllerRequest>,
    /// Handle for broadcasting events on the SSE stream. The handle is always present, but will
    /// only actively broadcast events when this controller is the leader.
    pub events_handle: EventsHandle,
    /// Used to record and report the servers current health.
    pub health_reporter: HealthReporter,
    /// Used to communicate externally that a failpoint request has been received and successfully
    /// handled.
    /// Most commonly used to block on further startup action if --wait-for-failpoint is supplied.
    #[allow(dead_code)]
    pub failpoint_channel: Option<Arc<Sender<()>>>,
}

impl NoriaServerHttpRouter {
    /// Creates a listener object to be used to route requests.
    pub async fn create_listener(&self) -> anyhow::Result<TcpListener> {
        let http_listener = TcpListener::bind(SocketAddr::new(self.listen_addr, self.port))
            .or_else(|_| {
                warn!(listen_addr = %self.listen_addr, port = self.port, "Could not bind to provided external port; using a random port instead");

                TcpListener::bind(SocketAddr::new(self.listen_addr, 0))
            })
            .await?;
        let _ = http_listener
            .local_addr()
            .inspect(|addr| info!(%addr, "Server listening for HTTP connections"));
        Ok(http_listener)
    }

    /// Routes requests for a noria server http router received on `http_listener`
    /// the service layer of the NoriaServerHttpRouter, see
    /// mpl Service<_> for NoriaServerHttpRouter.
    pub(crate) async fn route_requests(
        router: NoriaServerHttpRouter,
        http_listener: TcpListener,
        shutdown_rx: ShutdownReceiver,
    ) -> anyhow::Result<()> {
        hyper::server::Server::builder(hyper::server::accept::from_stream(
            shutdown_rx.wrap_stream(TcpListenerStream::new(http_listener)),
        ))
        .serve(make_service_fn(move |_| {
            let s = router.clone();
            async move { io::Result::Ok(s) }
        }))
        .map_err(move |e| anyhow!("HTTP server failed, {}", e))
        .await
    }
}

/// Tower service definition to route http requests `Request<Body>` to their
/// responses. Requests on the endpoint `/worker_request` are routed to the
/// worker along the `worker_tx` channel, while any request that is not specifically
/// handled by the http server directly is routed to the controller along the
/// `controller_tx` channel.
impl Service<Request<Body>> for NoriaServerHttpRouter {
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

        metrics::counter!(recorded::SERVER_EXTERNAL_REQUESTS).increment(1);

        match (req.method(), req.uri().path()) {
            #[cfg(feature = "failure_injection")]
            (&Method::POST, "/failpoint") => {
                let tx = self.failpoint_channel.clone();
                Box::pin(async move {
                    let body = hyper::body::to_bytes(req.into_body()).await.unwrap();
                    let contents = match bincode::deserialize(&body) {
                        Err(_) => {
                            return Ok(res
                                .status(StatusCode::BAD_REQUEST)
                                .header(CONTENT_TYPE, "text/plain")
                                .body(hyper::Body::from(
                                    "body cannot be deserialized into failpoint name and action",
                                ))
                                .unwrap());
                        }
                        Ok(contents) => contents,
                    };
                    let (name, action): (String, String) = contents;
                    let resp = res
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(
                            ::bincode::serialize(&fail::cfg(name, &action)).unwrap(),
                        ))
                        .unwrap();
                    if let Some(tx) = tx {
                        let _ = tx.send(()).await;
                    }
                    Ok(resp)
                })
            }
            (&Method::GET, "/graph.html") => {
                let res = res
                    .header(CONTENT_TYPE, "text/html")
                    .body(hyper::Body::from(include_str!("graph.html")));
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::GET, "/metrics") => {
                let render = get_global_recorder().map(|r| r.render());
                let res = res.header(CONTENT_TYPE, "text/plain");
                let res = match render {
                    Some(metrics) => res.body(hyper::Body::from(metrics)),
                    None => res
                        .status(StatusCode::NOT_FOUND)
                        .body(hyper::Body::from("Prometheus metrics were not enabled. To fix this, run Noria with --prometheus-metrics".to_string())),
                };
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::GET, "/health") => {
                let state = self.health_reporter.health().state;
                Box::pin(async move {
                    let body = format!("Server is in {} state", &state).into();
                    let res = match state {
                        State::Healthy | State::ShuttingDown => res
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(body),
                        _ => res
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(body),
                    };

                    Ok(res.unwrap())
                })
            }
            (&Method::POST, "/worker_request") => {
                metrics::counter!(recorded::SERVER_WORKER_REQUESTS).increment(1);

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
            // Returns a summary of memory usage for the entire process and per-thread memory usage
            (&Method::GET, "/memory_stats") => {
                let res = match print_memory_and_per_thread_stats() {
                    Ok(stats) => res
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(stats)),
                    Err(e) => res
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(format!(
                            "Error fetching memory stats: {e}"
                        ))),
                };

                Box::pin(async move { Ok(res.unwrap()) })
            }
            // Returns a large dump of jemalloc debugging information along with per-thread
            // memory stats
            (&Method::GET, "/memory_stats_verbose") => {
                let res = match dump_stats() {
                    Ok(stats) => res
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(stats)),
                    Err(e) => res
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(format!(
                            "Error fetching memory stats: {e}"
                        ))),
                };

                Box::pin(async move { Ok(res.unwrap()) })
            }
            // XXX: jemalloc profiling routes are duplicated across server and adapter so that they
            // are usable in distributed deployments, but in standalone deployments they will both
            // poke the same shared jemalloc allocator: there is no way to enable or disable
            // profiling on a crate-by-crate basis or anything like that.
            //
            // Turns on jemalloc's profiler
            (&Method::POST, "/jemalloc/profiling/activate") => {
                let res = match activate_prof() {
                    Ok(_) => res
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from("Memory profiling activated")),
                    Err(e) => res
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(format!(
                            "Error activating memory profiling: {e}"
                        ))),
                };
                Box::pin(async move { Ok(res.unwrap()) })
            }
            // Disables jemalloc's profiler
            (&Method::POST, "/jemalloc/profiling/deactivate") => {
                let res = match deactivate_prof() {
                    Ok(_) => res
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from("Memory profiling deactivated")),
                    Err(e) => res
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(format!(
                            "Error deactivating memory profiling: {e}"
                        ))),
                };
                Box::pin(async move { Ok(res.unwrap()) })
            }
            // Returns the current jemalloc profiler output
            (&Method::GET, "/jemalloc/profiling/dump") => Box::pin(async move {
                let res = match dump_prof_to_string().await {
                    Ok(dump) => res
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(dump)),
                    Err(e) => res
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(format!(
                            "Error dumping profiling output: {e}"
                        ))),
                };
                Ok(res.unwrap())
            }),
            // Sets the log level of the tracing system, similar to changing the `LOG_LEVEL`
            // environment variable.
            (&Method::POST, "/log_level") => Box::pin(async move {
                let bytes = hyper::body::to_bytes(req.into_body()).await.unwrap();
                let res = match str::from_utf8(&bytes) {
                    Err(e) => res
                        .status(StatusCode::BAD_REQUEST)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(format!("Invalid UTF-8: {e}"))),
                    Ok(directives) => match readyset_tracing::set_log_level(directives) {
                        Ok(_) => res
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(hyper::Body::from("Log level set successfully")),
                        Err(e) => res
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(hyper::Body::from(format!("Error setting log level: {e}"))),
                    },
                };
                Ok(res.unwrap())
            }),
            // SSE endpoint for schema catalog updates (or whatever events the server publishes)
            (&Method::GET, "/events/stream") => {
                let res = if let Some((events_rx, snapshot)) =
                    self.events_handle.subscribe_with_snapshot()
                {
                    // Emit the current schema catalog as the first SSE event so that
                    // clients which connect after a broadcast don't miss it. If
                    // serialization fails, return 500 rather than silently
                    // degrading to the pre-fix race condition.
                    let initial_sse = match SchemaCatalogUpdate::try_from(&*snapshot)
                        .and_then(|update| ControllerEvent::SchemaCatalogUpdate(update).to_sse())
                    {
                        Ok(sse) => sse,
                        Err(error) => {
                            return Box::pin(async move {
                                Ok(res
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .header(CONTENT_TYPE, "text/plain")
                                    .body(hyper::Body::from(format!(
                                        "Failed to serialize schema catalog snapshot: {error}"
                                    )))
                                    .unwrap())
                            });
                        }
                    };
                    let live_stream =
                        BroadcastStream::new(events_rx).filter_map(move |event| match event {
                            Ok(event) => match event.to_sse() {
                                Ok(s) => Some(Ok::<String, ReadySetError>(s)),
                                Err(error) => {
                                    warn!(
                                        %error,
                                        "Failed to serialize controller event for SSE"
                                    );
                                    None
                                }
                            },
                            Err(error) => {
                                warn!(%error, "Failed to receive controller event for SSE");
                                None
                            }
                        });
                    let combined =
                        tokio_stream::iter(Some(Ok::<String, ReadySetError>(initial_sse)))
                            .chain(live_stream);
                    let body = hyper::Body::wrap_stream(combined);
                    res.status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/event-stream")
                        .header(CACHE_CONTROL, "no-cache")
                        .body(body)
                } else {
                    res.status(StatusCode::SERVICE_UNAVAILABLE)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from(
                            "Leader not ready or we're not the leader",
                        ))
                };

                Box::pin(async move { Ok(res.unwrap()) })
            }
            _ => {
                metrics::counter!(recorded::SERVER_CONTROLLER_REQUESTS).increment(1);

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
