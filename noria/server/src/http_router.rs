use crate::controller::ControllerRequest;
use crate::metrics::{get_global_recorder, Clear, RecorderType};
use crate::worker::WorkerRequest;
use anyhow::anyhow;
use futures::TryFutureExt;
use hyper::header::CONTENT_TYPE;
use hyper::service::make_service_fn;
use hyper::{self, Body, Method, Request, Response, StatusCode};
use noria::consensus::{Authority, AuthorityControl};
use noria::metrics::recorded;
use noria::ReadySetError;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use stream_cancel::Valve;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;
use tracing::warn;

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
    /// The `Authority` used inside the server.
    pub authority: Arc<Authority>,
    /// A valve for the http stream to trigger closing.
    pub valve: Valve,
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
    pub async fn route_requests(
        router: NoriaServerHttpRouter,
        http_listener: TcpListener,
    ) -> anyhow::Result<()> {
        hyper::server::Server::builder(hyper::server::accept::from_stream(
            router.valve.wrap(TcpListenerStream::new(http_listener)),
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
