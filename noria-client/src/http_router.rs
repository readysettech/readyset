use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::TryFutureExt;
use hyper::header::CONTENT_TYPE;
use hyper::service::make_service_fn;
use hyper::{self, Body, Method, Request, Response};
use metrics_exporter_prometheus::PrometheusHandle;
use noria_client_metrics::recorded;
use stream_cancel::Valve;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;

use crate::query_status_cache::QueryStatusCache;

/// Routes requests from an HTTP server to expose metrics data from the adapter.
/// To see the supported http requests and their respective routing, see
/// impl Service<Request<Body>> for NoriaAdapterHttpRouter.
#[derive(Clone)]
pub struct NoriaAdapterHttpRouter {
    /// The address to attempt to listen on.
    pub listen_addr: SocketAddr,
    /// A reference to the QueryStatusCache that is in use by the adapter.
    pub query_cache: Arc<QueryStatusCache>,
    /// A valve for the http stream to trigger closing.
    pub valve: Valve,

    /// Used to retrive the prometheus scrape's render as a String when servicing
    /// HTTP requests on /prometheus.
    pub prometheus_handle: Option<PrometheusHandle>,
}

impl NoriaAdapterHttpRouter {
    /// Creates a listener object to be used to route requests.
    pub async fn create_listener(&self) -> anyhow::Result<TcpListener> {
        let http_listener = TcpListener::bind(self.listen_addr).await?;
        Ok(http_listener)
    }

    /// Routes requests for a noria adapter http router received on `http_listener`
    /// the service layer of the NoriaAdapterHttpRouter, see
    /// Impl Service<_> for NoriaAdapterHttpRouter.
    pub async fn route_requests(
        router: NoriaAdapterHttpRouter,
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
/// responses.
#[allow(clippy::type_complexity)] // No valid re-use to make this into custom type definitions.
impl Service<Request<Body>> for NoriaAdapterHttpRouter {
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

        metrics::increment_counter!(recorded::ADAPTER_EXTERNAL_REQUESTS);

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/allow-list") => {
                let query_cache = self.query_cache.clone();
                Box::pin(async move {
                    let allow_list = query_cache.allow_list();
                    let res = match serde_json::to_string(&allow_list) {
                        Ok(json) => res
                            .header(CONTENT_TYPE, "application/json")
                            .body(hyper::Body::from(json)),
                        Err(_) => res.status(500).header(CONTENT_TYPE, "text/plain").body(
                            hyper::Body::from(
                                "allow list failed to be converted into a json string".to_string(),
                            ),
                        ),
                    };
                    Ok(res.unwrap())
                })
            }
            (&Method::GET, "/deny-list") => {
                let query_cache = self.query_cache.clone();
                Box::pin(async move {
                    let deny_list = query_cache.deny_list();
                    let res = match serde_json::to_string(&deny_list) {
                        Ok(json) => res
                            .header(CONTENT_TYPE, "application/json")
                            .body(hyper::Body::from(json)),
                        Err(_) => res.status(500).header(CONTENT_TYPE, "text/plain").body(
                            hyper::Body::from(
                                "deny list failed to be converted into a json string".to_string(),
                            ),
                        ),
                    };
                    Ok(res.unwrap())
                })
            }
            (&Method::GET, "/health") => Box::pin(async move {
                let res = res
                    .status(200)
                    .header(CONTENT_TYPE, "text/plain")
                    .body(hyper::Body::empty());

                Ok(res.unwrap())
            }),
            (&Method::GET, "/prometheus") => {
                let body = self.prometheus_handle.as_ref().map(|x| x.render());
                let res = res.header(CONTENT_TYPE, "text/plain");
                let res = match body {
                    Some(metrics) => res.body(hyper::Body::from(metrics)),
                    None => res
                        .status(404)
                        .body(hyper::Body::from("Prometheus metrics were not enabled. To fix this, run the adapter with --prometheus-metrics".to_string())),
                };
                Box::pin(async move { Ok(res.unwrap()) })
            }
            _ => Box::pin(async move {
                let res = res
                    .status(404)
                    .header(CONTENT_TYPE, "text/plain")
                    .body(hyper::Body::empty());

                Ok(res.unwrap())
            }),
        }
    }
}
