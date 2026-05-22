use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::anyhow;
use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{Request, State};
use axum::http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{from_fn, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures::TryFutureExt;
use health_reporter::{HealthReporter, State as HealthState};
use readyset_alloc::{
    activate_prof, deactivate_prof, dump_prof_to_string, dump_stats,
    print_memory_and_per_thread_stats,
};
use readyset_client::events::ControllerEvent;
use readyset_client::metrics::recorded;
use readyset_errors::ReadySetError;
use readyset_metrics::metrics_body;
use readyset_util::shutdown::ShutdownReceiver;
use schema_catalog::SchemaCatalogUpdate;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use tracing::{info, warn};

use crate::controller::events::EventsHandle;
use crate::controller::ControllerRequest;
use crate::worker::{WorkerRequest, WorkerRequestKind};

/// Routes requests from an HTTP server to noria server workers and controllers.
///
/// `worker_tx` and `controller_tx` are mpsc senders this router uses to forward HTTP requests
/// to the worker and controller threads respectively. Requests on `/worker_request` go to the
/// worker; anything that doesn't match a well-known route falls through to the controller.
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

    /// Routes HTTP requests for a noria server router on `http_listener`.
    pub(crate) async fn route_requests(
        router: NoriaServerHttpRouter,
        http_listener: TcpListener,
        mut shutdown_rx: ShutdownReceiver,
    ) -> anyhow::Result<()> {
        let app = build_app(router);
        axum::serve(http_listener, app)
            .with_graceful_shutdown(async move { shutdown_rx.recv().await })
            .await
            .map_err(|e| anyhow!("HTTP server failed, {}", e))
    }
}

fn build_app(router: NoriaServerHttpRouter) -> Router {
    #[cfg_attr(not(feature = "failure_injection"), allow(unused_mut))]
    let mut app = Router::new()
        .route("/graph.html", get(graph_html))
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .route("/worker_request", post(worker_request))
        .route("/memory_stats", get(memory_stats))
        .route("/memory_stats_verbose", get(memory_stats_verbose))
        .route(
            "/jemalloc/profiling/activate",
            post(jemalloc_profiling_activate),
        )
        .route(
            "/jemalloc/profiling/deactivate",
            post(jemalloc_profiling_deactivate),
        )
        .route("/jemalloc/profiling/dump", get(jemalloc_profiling_dump))
        .route("/log_level", post(log_level))
        .route("/events/stream", get(events_stream));

    #[cfg(feature = "failure_injection")]
    {
        app = app.route("/failpoint", post(failpoint));
    }

    app.fallback(forward_to_controller)
        .layer(from_fn(record_external_request))
        .with_state(router)
}

/// Middleware: count every external request and tag responses with permissive CORS.
async fn record_external_request(req: Request, next: Next) -> Response {
    metrics::counter!(recorded::SERVER_EXTERNAL_REQUESTS).increment(1);
    let mut res = next.run(req).await;
    res.headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    res
}

fn text(status: StatusCode, body: impl Into<String>) -> Response {
    (status, [(CONTENT_TYPE, "text/plain")], body.into()).into_response()
}

fn octet_stream(status: StatusCode, body: Vec<u8>) -> Response {
    (
        status,
        [(CONTENT_TYPE, "application/octet-stream")],
        Bytes::from(body),
    )
        .into_response()
}

async fn graph_html() -> Response {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/html")],
        include_str!("graph.html"),
    )
        .into_response()
}

async fn metrics() -> Response {
    match metrics_body() {
        Ok(body) => text(StatusCode::OK, body),
        Err(msg) => text(StatusCode::NOT_FOUND, msg),
    }
}

async fn health(State(state): State<NoriaServerHttpRouter>) -> Response {
    let s = state.health_reporter.health().state;
    let body = format!("Server is in {} state", &s);
    let status = match s {
        HealthState::Healthy | HealthState::ShuttingDown => StatusCode::OK,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    text(status, body)
}

async fn worker_request(State(state): State<NoriaServerHttpRouter>, body: Bytes) -> Response {
    metrics::counter!(recorded::SERVER_WORKER_REQUESTS).increment(1);

    let wrq: WorkerRequestKind = match bincode::deserialize(&body) {
        Ok(x) => x,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(CONTENT_TYPE, "text/plain; charset=utf-8")],
                Bytes::from(bincode::serialize(&ReadySetError::from(e)).unwrap()),
            )
                .into_response();
        }
    };

    // Convert the wire-encoded `WorkerRequestKind` into a typed `WorkerRequest`, await the
    // typed response, then bincode-wrap it as `ReadySetResult<Option<Vec<u8>>>` to preserve
    // the HTTP route's response contract. Piece B/C of the Phase 3 cleanup deletes this
    // bridge once nothing relies on `/worker_request` over the wire.
    let result = dispatch_worker_request(&state.worker_tx, wrq).await;

    match result {
        Ok(Some(bytes)) => octet_stream(StatusCode::OK, bytes),
        Ok(None) => octet_stream(StatusCode::OK, bincode::serialize(&()).unwrap()),
        Err(DispatchError::ChannelClosed) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(DispatchError::WorkerError(e)) => octet_stream(
            StatusCode::INTERNAL_SERVER_ERROR,
            bincode::serialize(&e).unwrap(),
        ),
    }
}

enum DispatchError {
    ChannelClosed,
    WorkerError(ReadySetError),
}

async fn dispatch_worker_request(
    worker_tx: &Sender<WorkerRequest>,
    kind: WorkerRequestKind,
) -> Result<Option<Vec<u8>>, DispatchError> {
    // Per-variant typed oneshots flatten into `Option<Vec<u8>>`: `None` for unit-returning
    // variants (the HTTP path historically returned `bincode::serialize(&())` for those),
    // `Some(bytes)` for `DomainRequest`.
    match kind {
        WorkerRequestKind::RunDomain(builder) => {
            let (done_tx, done_rx) = oneshot::channel();
            send_with_close_check(
                worker_tx,
                WorkerRequest::RunDomain { builder, done_tx },
                done_rx,
            )
            .await
            .map(|()| None)
        }
        WorkerRequestKind::ClearDomains => {
            let (done_tx, done_rx) = oneshot::channel();
            send_with_close_check(worker_tx, WorkerRequest::ClearDomains { done_tx }, done_rx)
                .await
                .map(|()| None)
        }
        WorkerRequestKind::KillDomains(domains) => {
            let (done_tx, done_rx) = oneshot::channel();
            send_with_close_check(
                worker_tx,
                WorkerRequest::KillDomains { domains, done_tx },
                done_rx,
            )
            .await
            .map(|()| None)
        }
        WorkerRequestKind::DomainRequest {
            domain_index,
            request,
        } => {
            let (done_tx, done_rx) = oneshot::channel();
            send_with_close_check(
                worker_tx,
                WorkerRequest::DomainRequest {
                    domain_index,
                    request,
                    done_tx,
                },
                done_rx,
            )
            .await
            .map(Some)
        }
        WorkerRequestKind::SetMemoryLimit { period, limit } => {
            let (done_tx, done_rx) = oneshot::channel();
            send_with_close_check(
                worker_tx,
                WorkerRequest::SetMemoryLimit {
                    period,
                    limit,
                    done_tx,
                },
                done_rx,
            )
            .await
            .map(|()| None)
        }
        WorkerRequestKind::BarrierCredit { id, credits } => {
            let (done_tx, done_rx) = oneshot::channel();
            send_with_close_check(
                worker_tx,
                WorkerRequest::BarrierCredit {
                    id,
                    credits,
                    done_tx,
                },
                done_rx,
            )
            .await
            .map(|()| None)
        }
    }
}

async fn send_with_close_check<T>(
    worker_tx: &Sender<WorkerRequest>,
    req: WorkerRequest,
    done_rx: oneshot::Receiver<readyset_errors::ReadySetResult<T>>,
) -> Result<T, DispatchError> {
    if worker_tx.send(req).await.is_err() {
        return Err(DispatchError::ChannelClosed);
    }
    match done_rx.await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(DispatchError::WorkerError(e)),
        Err(_) => Err(DispatchError::ChannelClosed),
    }
}

async fn memory_stats() -> Response {
    match print_memory_and_per_thread_stats() {
        Ok(stats) => text(StatusCode::OK, stats),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error fetching memory stats: {e}"),
        ),
    }
}

async fn memory_stats_verbose() -> Response {
    match dump_stats() {
        Ok(stats) => text(StatusCode::OK, stats),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error fetching memory stats: {e}"),
        ),
    }
}

// XXX: jemalloc profiling routes are duplicated across server and adapter so that they
// are usable in distributed deployments, but in standalone deployments they will both
// poke the same shared jemalloc allocator: there is no way to enable or disable
// profiling on a crate-by-crate basis or anything like that.

async fn jemalloc_profiling_activate() -> Response {
    match activate_prof() {
        Ok(_) => text(StatusCode::OK, "Memory profiling activated"),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error activating memory profiling: {e}"),
        ),
    }
}

async fn jemalloc_profiling_deactivate() -> Response {
    match deactivate_prof() {
        Ok(_) => text(StatusCode::OK, "Memory profiling deactivated"),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error deactivating memory profiling: {e}"),
        ),
    }
}

async fn jemalloc_profiling_dump() -> Response {
    match dump_prof_to_string().await {
        Ok(dump) => text(StatusCode::OK, dump),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error dumping profiling output: {e}"),
        ),
    }
}

async fn log_level(body: Bytes) -> Response {
    match str::from_utf8(&body) {
        Err(e) => text(StatusCode::BAD_REQUEST, format!("Invalid UTF-8: {e}")),
        Ok(directives) => match readyset_tracing::set_log_level(directives) {
            Ok(_) => text(StatusCode::OK, "Log level set successfully"),
            Err(e) => text(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error setting log level: {e}"),
            ),
        },
    }
}

async fn events_stream(State(state): State<NoriaServerHttpRouter>) -> Response {
    let Some((events_rx, snapshot)) = state.events_handle.subscribe_with_snapshot() else {
        return text(
            StatusCode::SERVICE_UNAVAILABLE,
            "Leader not ready or we're not the leader",
        );
    };

    // Emit the current schema catalog as the first SSE event so that clients which connect
    // after a broadcast don't miss it. If serialization fails, return 500 rather than silently
    // degrading to the pre-fix race condition.
    let initial_sse = match SchemaCatalogUpdate::try_from(&*snapshot)
        .and_then(|update| ControllerEvent::SchemaCatalogUpdate(update).to_sse())
    {
        Ok(sse) => sse,
        Err(error) => {
            return text(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to serialize schema catalog snapshot: {error}"),
            );
        }
    };

    let live_stream = BroadcastStream::new(events_rx).filter_map(|event| match event {
        Ok(event) => match event.to_sse() {
            Ok(s) => Some(Ok::<String, Infallible>(s)),
            Err(error) => {
                warn!(%error, "Failed to serialize controller event for SSE");
                None
            }
        },
        Err(error) => {
            warn!(%error, "Failed to receive controller event for SSE");
            None
        }
    });
    let combined =
        tokio_stream::iter(Some(Ok::<String, Infallible>(initial_sse))).chain(live_stream);
    let body = Body::from_stream(combined);

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .header(CACHE_CONTROL, "no-cache")
        .body(body)
        .expect("static SSE response is well-formed")
}

#[cfg(feature = "failure_injection")]
async fn failpoint(State(state): State<NoriaServerHttpRouter>, body: Bytes) -> Response {
    let (name, action): (String, String) = match bincode::deserialize(&body) {
        Ok(v) => v,
        Err(_) => {
            return text(
                StatusCode::BAD_REQUEST,
                "body cannot be deserialized into failpoint name and action",
            );
        }
    };
    let payload = ::bincode::serialize(&fail::cfg(name, &action))
        .expect("failpoint result is bincode-serializable");
    if let Some(tx) = state.failpoint_channel.as_ref() {
        let _ = tx.send(()).await;
    }
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain")],
        Bytes::from(payload),
    )
        .into_response()
}

/// Catch-all: forward to the controller via the `controller_tx` channel and relay the response.
async fn forward_to_controller(
    State(state): State<NoriaServerHttpRouter>,
    req: Request,
) -> Response {
    metrics::counter!(recorded::SERVER_CONTROLLER_REQUESTS).increment(1);

    let (parts, body) = req.into_parts();
    let body = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            return text(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read request body: {e}"),
            );
        }
    };

    let (tx, rx) = oneshot::channel();
    let req = ControllerRequest {
        method: parts.method,
        path: parts.uri.path().to_string(),
        query: parts.uri.query().map(ToOwned::to_owned),
        body,
        reply_tx: tx,
    };

    if state.controller_tx.send(req).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(CONTENT_TYPE, "text/plain; charset=utf-8")],
            "server went away",
        )
            .into_response();
    }

    match rx.await {
        Ok(Ok(Ok(reply))) => octet_stream(StatusCode::OK, reply),
        Ok(Ok(Err(reply))) => octet_stream(StatusCode::INTERNAL_SERVER_ERROR, reply),
        Ok(Err(status)) => status.into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
