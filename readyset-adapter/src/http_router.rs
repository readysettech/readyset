use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::anyhow;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use health_reporter::{HealthReporter as AdapterHealthReporter, State as HealthState};
use metrics::{Gauge, counter, gauge};
use readyset_alloc::{
    activate_prof, deactivate_prof, dump_prof_to_string, dump_stats,
    print_memory_and_per_thread_stats,
};
use readyset_metrics::metrics_body;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tracing::info;

use crate::UpstreamDatabase;
use crate::status_reporter::ReadySetStatusReporter;

/// Routes requests from an HTTP server to expose metrics data from the adapter.
///
/// See `register_routes` for the supported routes and their behavior.
pub struct NoriaAdapterHttpRouter<U>
where
    U: UpstreamDatabase,
{
    /// The address to attempt to listen on.
    pub listen_addr: SocketAddr,
    /// Used to retrieve the current health of the adapter.
    pub health_reporter: AdapterHealthReporter,
    /// Used to communicate externally that a failpoint request has been received and successfully
    /// handled.
    /// Most commonly used to block on further startup action if --wait-for-failpoint is supplied
    /// to the adapter.
    pub failpoint_channel: Option<Arc<Sender<()>>>,

    /// Used to record metrics related to http request handling.
    pub metrics: HttpRouterMetrics,

    pub status_reporter: ReadySetStatusReporter<U>,
}

// For some reason, the default implementation, which should match this, isn't compiling.
impl<U> Clone for NoriaAdapterHttpRouter<U>
where
    U: UpstreamDatabase,
{
    fn clone(&self) -> Self {
        Self {
            listen_addr: self.listen_addr,
            health_reporter: self.health_reporter.clone(),
            failpoint_channel: self.failpoint_channel.clone(),
            metrics: self.metrics.clone(),
            status_reporter: self.status_reporter.clone(),
        }
    }
}

impl<U> NoriaAdapterHttpRouter<U>
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    /// Creates a listener object to be used to route requests.
    pub async fn create_listener(&self) -> anyhow::Result<TcpListener> {
        let http_listener = TcpListener::bind(self.listen_addr).await?;
        let _ = http_listener
            .local_addr()
            .inspect(|addr| info!(%addr, "Adapter listening for HTTP connections"));
        Ok(http_listener)
    }

    /// Routes requests for the adapter HTTP router on `http_listener`.
    pub async fn route_requests(
        router: NoriaAdapterHttpRouter<U>,
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

fn build_app<U>(router: NoriaAdapterHttpRouter<U>) -> Router
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    #[cfg_attr(not(feature = "failure_injection"), allow(unused_mut))]
    let mut app = Router::new()
        .route("/health", get(health::<U>))
        .route("/metrics", get(metrics::<U>))
        .route("/readyset_status", get(readyset_status::<U>))
        .route("/memory_stats", get(memory_stats::<U>))
        .route("/memory_stats_verbose", get(memory_stats_verbose::<U>))
        .route(
            "/jemalloc/profiling/activate",
            post(jemalloc_profiling_activate::<U>),
        )
        .route(
            "/jemalloc/profiling/deactivate",
            post(jemalloc_profiling_deactivate::<U>),
        )
        .route(
            "/jemalloc/profiling/dump",
            get(jemalloc_profiling_dump::<U>),
        )
        .route("/log_level", post(log_level::<U>));

    #[cfg(feature = "failure_injection")]
    {
        app = app.route("/failpoint", post(failpoint::<U>));
    }

    app.fallback(not_found)
        .layer(from_fn(record_external_request))
        .with_state(router)
}

/// Middleware: count every external request and tag responses with permissive CORS so the adapter
/// can be probed from a browser.
async fn record_external_request(req: Request, next: Next) -> Response {
    counter!(metric::ADAPTER_EXTERNAL_REQUESTS).increment(1);
    let mut res = next.run(req).await;
    res.headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    res
}

fn text(status: StatusCode, body: impl Into<String>) -> Response {
    (status, [(CONTENT_TYPE, "text/plain")], body.into()).into_response()
}

/// # ReadySet Adapter Endpoints
///
/// The following HTTP endpoints are exposed by the ReadySet Adapter.
///
/// ## Health Check
///
/// `GET /health` returns 200 if the adapter is healthy or shutting down, and 500 otherwise. The
/// router being reachable does not imply the SQL listener is healthy — see
/// `health_reporter::HealthReporter` for what "healthy" means here.
async fn health<U>(State(state): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    let s = state.health_reporter.health().state;
    let body = format!("Adapter is in {} state", &s);
    let status = match s {
        HealthState::Healthy | HealthState::ShuttingDown => StatusCode::OK,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    text(status, body)
}

/// `GET /metrics` returns the Prometheus exposition payload, or 404 if the adapter was started
/// without `--prometheus-metrics`. Intended for Prometheus scraping.
async fn metrics<U>(State(state): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    match metrics_body() {
        Ok(payload) => {
            state.metrics.rec_metrics_payload_size(payload.len());
            text(StatusCode::OK, payload)
        }
        Err(msg) => text(StatusCode::NOT_FOUND, msg),
    }
}

/// `GET /readyset_status` returns a JSON status report aggregated by the status reporter.
async fn readyset_status<U>(State(state): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    let body = state.status_reporter.report_status().await.into_json();
    text(StatusCode::OK, body)
}

/// `GET /memory_stats` returns a summary of process and per-thread memory usage.
async fn memory_stats<U>(State(_): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    match print_memory_and_per_thread_stats() {
        Ok(stats) => text(StatusCode::OK, stats),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error fetching memory stats: {e}"),
        ),
    }
}

/// `GET /memory_stats_verbose` dumps full jemalloc stats plus per-thread memory stats.
async fn memory_stats_verbose<U>(State(_): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
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

/// `POST /jemalloc/profiling/activate` turns on jemalloc's profiler.
async fn jemalloc_profiling_activate<U>(State(_): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    match activate_prof() {
        Ok(_) => text(StatusCode::OK, "Memory profiling activated"),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error activating memory profiling: {e}"),
        ),
    }
}

/// `POST /jemalloc/profiling/deactivate` turns off jemalloc's profiler.
async fn jemalloc_profiling_deactivate<U>(State(_): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    match deactivate_prof() {
        Ok(_) => text(StatusCode::OK, "Memory profiling deactivated"),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error deactivating memory profiling: {e}"),
        ),
    }
}

/// `GET /jemalloc/profiling/dump` returns the current jemalloc profiler output.
async fn jemalloc_profiling_dump<U>(State(_): State<NoriaAdapterHttpRouter<U>>) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    match dump_prof_to_string().await {
        Ok(dump) => text(StatusCode::OK, dump),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error dumping profiling output: {e}"),
        ),
    }
}

/// `POST /log_level` updates the tracing filter directives, mirroring the `LOG_LEVEL` env var.
async fn log_level<U>(State(_): State<NoriaAdapterHttpRouter<U>>, body: Bytes) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
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

#[cfg(feature = "failure_injection")]
async fn failpoint<U>(State(state): State<NoriaAdapterHttpRouter<U>>, body: Bytes) -> Response
where
    U: UpstreamDatabase + Send + Sync + 'static,
{
    use bincode::{deserialize, serialize};
    use fail::cfg;

    let (name, action): (String, String) = match deserialize(&body) {
        Ok(v) => v,
        Err(_) => {
            return text(
                StatusCode::BAD_REQUEST,
                "body cannot be deserialized into failpoint name and action",
            );
        }
    };
    let payload = serialize(&cfg(name, &action)).expect("failpoint result is bincode-serializable");
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

async fn not_found() -> Response {
    text(StatusCode::NOT_FOUND, String::new())
}

#[derive(Clone)]
pub struct HttpRouterMetrics {
    /// The last seen size of the /metrics endpoint payload, in bytes
    metrics_payload_size: Gauge,
}

impl Default for HttpRouterMetrics {
    fn default() -> Self {
        Self {
            metrics_payload_size: gauge!(metric::METRICS_PAYLOAD_SIZE_BYTES),
        }
    }
}

impl HttpRouterMetrics {
    /// Record the size of the /metrics payload in bytes
    pub(super) fn rec_metrics_payload_size(&self, payload_size: usize) {
        self.metrics_payload_size.set(payload_size as f64);
    }
}
