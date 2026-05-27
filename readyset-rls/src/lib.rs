//! Postgres Row Level Security support for Readyset's shallow cache.
//!
//! - [`PolicyRegistry`] holds the catalog snapshot (per-relation policies, per-table RLS flags,
//!   per-role attributes, view dependencies, role-default GUCs) plus a monotonic `generation` that
//!   drives the shallow-cache `deps_gen` lookup field.
//! - [`analyzer`] walks parsed Postgres policy expressions and decides whether a query referencing
//!   them is cacheable, refusing anything outside the supported grammar envelope.
//! - [`loader`] and [`poller`] open a `REPEATABLE READ` snapshot on first start and periodically
//!   fingerprint `pg_policy` / `pg_class` / `pg_roles` to detect DDL.
//!
//! Postgres-only. MySQL flows do not touch this crate.

pub mod analyzer;
pub mod loader;
pub mod policy_registry;
pub mod poller;
pub mod types;

pub use analyzer::{CacheSessionDeps, Cacheability, RefuseReason, analyze_cache};
pub use policy_registry::PolicyRegistry;
pub use poller::{ChangeEvent, ChangeKind, Fingerprints, PollerHealth, run_poller};
pub use types::{FunctionMeta, RlsConfig, SessionInputType};

/// Postgres object identifier (a `pg_catalog` OID). RLS-specific; the generic shallow cache has no
/// notion of catalog identity, so the type lives here rather than in `readyset-shallow`.
pub type Oid = u32;

/// Sink the catalog poller drives when a change lands. Implemented by the adapter, which owns the
/// cache-create wiring and the relation/role -> cache reverse indices.
///
/// A bumped registry generation already invalidates entries keyed under the old generation (the
/// adapter folds the generation into the cache key), so these callbacks cover only what the
/// generation bump cannot: re-deriving a cache's keyed set when its policy changes, and dropping
/// pre-existing plain caches when a relation turns RLS-active.
pub trait InvalidationSink: Send + Sync {
    /// A relation's policy changed: re-analyze the caches over `relid`, re-keying those still in
    /// grammar and dropping those that left it.
    fn on_relation_changed(&self, relid: Oid);
    /// A role's `rolbypassrls` / `rolsuper` changed.
    fn on_role_changed(&self, roleid: Oid);
    /// RLS was enabled on `relid`: drop the pre-existing plain caches over it so they re-enter and
    /// come back scoped if still cacheable.
    fn on_rls_flag_enabled(&self, relid: Oid);
}

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use metrics::counter;

/// An [`InvalidationSink`] whose real target is installed after construction.
///
/// The adapter's real sink (the `RlsCoordinator`) needs the [`PolicyRegistry`] to exist first, but
/// bootstrap creates the registry and starts the poller in one call. This breaks the cycle: pass a
/// `DeferredSink` to bootstrap, then install the coordinator once the registry is in hand.
/// Callbacks before installation are dropped, which is safe because no cache can have been
/// registered yet.
#[derive(Default)]
pub struct DeferredSink {
    inner: OnceLock<Arc<dyn InvalidationSink>>,
}

impl std::fmt::Debug for DeferredSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredSink")
            .field("installed", &self.inner.get().is_some())
            .finish()
    }
}

impl DeferredSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the real sink. Idempotent: a second call is ignored.
    pub fn set(&self, sink: Arc<dyn InvalidationSink>) {
        let _ = self.inner.set(sink);
    }
}

impl InvalidationSink for DeferredSink {
    fn on_relation_changed(&self, relid: Oid) {
        if let Some(sink) = self.inner.get() {
            sink.on_relation_changed(relid);
        }
    }
    fn on_role_changed(&self, roleid: Oid) {
        if let Some(sink) = self.inner.get() {
            sink.on_role_changed(roleid);
        }
    }
    fn on_rls_flag_enabled(&self, relid: Oid) {
        if let Some(sink) = self.inner.get() {
            sink.on_rls_flag_enabled(relid);
        }
    }
}

/// Maximum number of `connect_with_driver` attempts made during adapter startup before bootstrap
/// aborts.
const BOOTSTRAP_MAX_ATTEMPTS: u32 = 5;

/// Initial backoff before the second bootstrap attempt; doubles each subsequent retry, capped at
/// `BOOTSTRAP_BACKOFF_MAX`.
const BOOTSTRAP_BACKOFF_INITIAL: Duration = Duration::from_millis(200);
const BOOTSTRAP_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Connect to `upstream_url`, retrying transient connect failures, then call [`bootstrap`].
///
/// Returns `Ok(None)` for non-Postgres URLs; MySQL deployments leave the registry empty and the
/// analyzer reports Cacheable for every query.
///
/// The first attempt fires immediately; each subsequent attempt waits an exponential backoff,
/// capped at `BOOTSTRAP_BACKOFF_MAX`. After [`BOOTSTRAP_MAX_ATTEMPTS`] failures the last
/// `BootstrapError` is returned so the caller can decide whether to abort.
pub async fn bootstrap_from_url(
    upstream_url: &str,
    config: RlsConfig,
    sink: Option<Arc<dyn InvalidationSink>>,
) -> Result<Option<BootstrapHandle>, BootstrapError> {
    if !upstream_url.starts_with("postgres://") && !upstream_url.starts_with("postgresql://") {
        return Ok(None);
    }

    let mut backoff = BOOTSTRAP_BACKOFF_INITIAL;
    let mut last_err: Option<ConnectError> = None;
    let mut client = None;
    for attempt in 1..=BOOTSTRAP_MAX_ATTEMPTS {
        counter!(metric::RLS_BOOTSTRAP_ATTEMPTS_TOTAL).increment(1);
        match connect_with_driver(upstream_url).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(e) => {
                tracing::warn!(
                    attempt,
                    max = BOOTSTRAP_MAX_ATTEMPTS,
                    error = %e,
                    "RLS bootstrap connect attempt failed; retrying"
                );
                last_err = Some(e);
                if attempt < BOOTSTRAP_MAX_ATTEMPTS {
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff.saturating_mul(2), BOOTSTRAP_BACKOFF_MAX);
                }
            }
        }
    }
    let client = match client {
        Some(c) => c,
        None => {
            return Err(BootstrapError::Connect(
                last_err.expect("loop ran at least once with last_err recorded"),
            ));
        }
    };

    let upstream_url = upstream_url.to_owned();
    let reconnect = move || {
        let url = upstream_url.clone();
        async move { connect_with_driver(&url).await }
    };
    let handle = bootstrap(client, config, sink, reconnect).await?;
    Ok(Some(handle))
}

/// Handle returned from a successful bootstrap. Holds the shared registry and the watch sender that
/// stops the poller.
///
/// The caller must keep this handle alive for as long as the poller should run: `shutdown_tx` is
/// the poller's only shutdown sender, so dropping the handle closes the watch channel and the
/// poller exits on its next tick. To stop cleanly, send `true` on `shutdown_tx` so the current tick
/// drains its in-flight upstream work before the loop returns.
#[derive(Debug)]
pub struct BootstrapHandle {
    pub registry: Arc<PolicyRegistry>,
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
}

async fn connect_with_driver(upstream_url: &str) -> Result<tokio_postgres::Client, ConnectError> {
    let tls_inner = native_tls::TlsConnector::builder()
        .build()
        .map_err(ConnectError::Tls)?;
    let tls = postgres_native_tls::MakeTlsConnector::new(tls_inner);
    let (client, connection) = tokio_postgres::connect(upstream_url, tls)
        .await
        .map_err(ConnectError::Postgres)?;
    // Drive the connection on a detached task; it ends when the client is dropped or the upstream
    // tears down the socket, after which the poller's next `is_closed()` check triggers a
    // reconnect.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "RLS upstream connection ended");
        }
    });
    Ok(client)
}

/// One attempt at opening an upstream connection failed. Distinguishes a Postgres-level failure
/// (network blip, auth) from a native TLS stack initialisation failure, so operators reading logs
/// know which class of failure to triage.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("native TLS initialisation failed: {0}")]
    Tls(native_tls::Error),
    #[error(transparent)]
    Postgres(tokio_postgres::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("RLS upstream connect failed: {0}")]
    Connect(ConnectError),
    #[error(transparent)]
    Loader(#[from] loader::LoaderError),
}

pub async fn bootstrap<F, Fut>(
    client: tokio_postgres::Client,
    config: RlsConfig,
    sink: Option<Arc<dyn InvalidationSink>>,
    reconnect: F,
) -> Result<BootstrapHandle, loader::LoaderError>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut:
        std::future::Future<Output = Result<tokio_postgres::Client, ConnectError>> + Send + 'static,
{
    let registry = Arc::new(PolicyRegistry::new());
    loader::load_snapshot(&client, &registry).await?;
    let registry_for_task = Arc::clone(&registry);
    let health_for_task = Arc::new(PollerHealth::default());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        run_poller(
            client,
            registry_for_task,
            config,
            health_for_task,
            sink,
            reconnect,
            shutdown_rx,
        )
        .await;
    });
    Ok(BootstrapHandle {
        registry,
        shutdown_tx,
    })
}
