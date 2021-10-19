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
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tower::Service;

use crate::query_status_cache::QueryStatusCache;
use noria_client_metrics::recorded;

/// Routes requests from an HTTP server to expose metrics data from the adapter.
/// To see the supported http requests and their respective routing, see
/// impl Service<Request<Body>> for NoriaAdapterHttpRouter.
#[derive(Clone)]
pub struct NoriaAdapterHttpRouter {
    /// The address to attempt to listen on.
    pub listen_addr: SocketAddr,
    /// A reference to the QueryStatusCache that is in use by the adapter, if it's enabled.
    pub query_cache: Option<Arc<QueryStatusCache>>,
    /// A valve for the http stream to trigger closing.
    pub valve: Valve,
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

        match (&self.query_cache, req.method(), req.uri().path()) {
            (Some(query_cache), &Method::GET, "/allow-list") => {
                let query_cache = query_cache.clone();
                Box::pin(async move {
                    let allow_list = query_cache.allow_list().await;
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
            (Some(query_cache), &Method::GET, "/deny-list") => {
                let query_cache = query_cache.clone();
                Box::pin(async move {
                    let deny_list = query_cache.deny_list().await;
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
