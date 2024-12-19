use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::TryFutureExt;
use health_reporter::{HealthReporter, State};
use hyper::header::CONTENT_TYPE;
use hyper::service::make_service_fn;
use hyper::{self, Body, Method, Request, Response, StatusCode};
use readyset_alloc::{dump_stats, print_memory_and_per_thread_stats};
use readyset_client::metrics::recorded;
use readyset_errors::ReadySetError;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;
use tracing::warn;

use crate::controller::ControllerRequest;
use crate::metrics::{get_global_recorder, Clear, RecorderType};
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
                warn!("Could not bind to provided external port; using a random port instead!");

                TcpListener::bind(SocketAddr::new(self.listen_addr, 0))
            })
            .await?;
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
            (&Method::GET, "/failpoint") => {
                let tx = self.failpoint_channel.clone();
                Box::pin(async move {
                    let body = hyper::body::to_bytes(req.into_body()).await.unwrap();
                    let contents = match bincode::deserialize(&body) {
                        Err(_) => {
                            return Ok(res
                                .status(400)
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
                        .status(200)
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
                let render = get_global_recorder().and_then(|r| r.render(RecorderType::Prometheus));
                let res = res.header(CONTENT_TYPE, "text/plain");
                let res = match render {
                    Some(metrics) => res.body(hyper::Body::from(metrics)),
                    None => res
                        .status(404)
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
                            .status(200)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(body),
                        _ => res
                            .status(500)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(body),
                    };

                    Ok(res.unwrap())
                })
            }
            (&Method::POST, "/metrics_dump") => {
                let render = get_global_recorder().and_then(|r| r.render(RecorderType::Noria));
                let res = match render {
                    Some(metrics) => res
                        .header(CONTENT_TYPE, "application/json")
                        .body(hyper::Body::from(metrics)),
                    None => res
                        .status(404)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(hyper::Body::from("Noria metrics were not enabled. To fix this, run Noria with --noria-metrics".to_string())),
                };
                Box::pin(async move { Ok(res.unwrap()) })
            }
            (&Method::POST, "/reset_metrics") => {
                if let Some(r) = get_global_recorder() {
                    r.clear();
                }
                let res = res
                    .header(CONTENT_TYPE, "application/json")
                    .body(hyper::Body::from(vec![]));
                Box::pin(async move { Ok(res.unwrap()) })
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
            (&Method::POST, "/memory_stats") => {
                let res =
                    match print_memory_and_per_thread_stats() {
                        Ok(stats) => res
                            .status(200)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(hyper::Body::from(stats)),
                        Err(e) => res.status(500).header(CONTENT_TYPE, "text/plain").body(
                            hyper::Body::from(format!("Error fetching memory stats: {e}")),
                        ),
                    };

                Box::pin(async move { Ok(res.unwrap()) })
            }
            // Returns a large dump of jemalloc debugging information along with per-thread
            // memory stats
            (&Method::POST, "/memory_stats_verbose") => {
                let res =
                    match dump_stats() {
                        Ok(stats) => res
                            .status(200)
                            .header(CONTENT_TYPE, "text/plain")
                            .body(hyper::Body::from(stats)),
                        Err(e) => res.status(500).header(CONTENT_TYPE, "text/plain").body(
                            hyper::Body::from(format!("Error fetching memory stats: {e}")),
                        ),
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
