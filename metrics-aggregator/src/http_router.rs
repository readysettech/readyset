use anyhow::anyhow;
use futures::TryFutureExt;
use hyper::{self, header::CONTENT_TYPE, Method};
use hyper::{service::make_service_fn, Body, Request, Response};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use stream_cancel::Valve;
use tokio::{net::TcpListener, sync::Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;

use crate::QueryMetricsCache;

/// Routes requests from an HTTP server to expose metrics data from the adapter.
/// To see the supported http requests and their respective routing, see
/// impl Service<Request<Body>> for MetricsAggregatorHttpRouter.
#[derive(Clone)]
pub struct MetricsAggregatorHttpRouter {
    /// The address to attempt to listen on.
    pub listen_addr: SocketAddr,
    /// A reference to the QueryMetricsCache that is in use.
    pub query_metrics_cache: Arc<Mutex<QueryMetricsCache>>,
    /// A valve for the http stream to trigger closing.
    pub valve: Valve,
}

impl MetricsAggregatorHttpRouter {
    /// Creates a listener object to be used to route requests.
    pub async fn create_listener(&self) -> anyhow::Result<TcpListener> {
        let http_listener = TcpListener::bind(self.listen_addr).await?;
        Ok(http_listener)
    }

    /// Routes requests for a noria adapter http router received on `http_listener`
    /// the service layer of the MetricsAggregatorHttpRouter, see
    /// Impl Service<_> for MetricsAggregatorHttpRouter.
    pub async fn route_requests(
        router: MetricsAggregatorHttpRouter,
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
impl Service<Request<Body>> for MetricsAggregatorHttpRouter {
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

        match (req.method(), req.uri().path()) {
            (&Method::GET, "/allow-list") => {
                let cache = self.query_metrics_cache.clone();
                Box::pin(async move {
                    let res = match serde_json::to_string(&cache.lock().await.allow_list) {
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
                let cache = self.query_metrics_cache.clone();
                Box::pin(async move {
                    let res = match serde_json::to_string(&cache.lock().await.deny_list) {
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
