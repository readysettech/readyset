use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::future;
use futures_util::future::Either;
use hyper::client::HttpConnector;
use noria_errors::{
    internal, internal_err, rpc_err, rpc_err_no_downcast, ReadySetError, ReadySetResult,
};
use parking_lot::RwLock;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use tower::buffer::Buffer;
use tower::ServiceExt;
use tower_service::Service;
use tracing::trace;
use url::Url;

use crate::consensus::{Authority, AuthorityControl};
use crate::debug::info::GraphInfo;
use crate::debug::stats;
use crate::metrics::MetricsDump;
use crate::replication::ReplicationOffsets;
use crate::status::ReadySetStatus;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::{
    ActivationResult, ReaderReplicationResult, ReaderReplicationSpec, RecipeSpec,
    ReplicationOffset, ViewFilter, ViewRequest,
};

/// Describes a running controller instance.
///
/// A serialized version of this struct is stored in ZooKeeper so that clients can reach the
/// currently active controller.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[doc(hidden)]
pub struct ControllerDescriptor {
    pub controller_uri: Url,
    pub nonce: u64,
}

struct Controller {
    authority: Arc<Authority>,
    client: hyper::Client<hyper::client::HttpConnector>,
    /// The last valid leader URL seen by this service. Used to circumvent requests to Consul in
    /// the happy-path.
    leader_url: Arc<RwLock<Option<Url>>>,
}

#[derive(Debug)]
struct ControllerRequest {
    path: &'static str,
    request: Vec<u8>,
    timeout: Option<Duration>,
}

impl ControllerRequest {
    fn new<Q: Serialize>(
        path: &'static str,
        r: Q,
        timeout: Option<Duration>,
    ) -> ReadySetResult<Self> {
        Ok(ControllerRequest {
            path,
            request: bincode::serialize(&r)?,
            timeout,
        })
    }
}

impl Service<ControllerRequest> for Controller {
    type Response = hyper::body::Bytes;
    type Error = ReadySetError;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ControllerRequest) -> Self::Future {
        trace!(?req, "Issuing controller RPC");
        let client = self.client.clone();
        let auth = self.authority.clone();
        let leader_url = self.leader_url.clone();
        let request_timeout = req.timeout.unwrap_or(Duration::MAX);
        let path = req.path;
        let body = req.request;
        let start = Instant::now();
        let mut last_error_desc: Option<String> = None;

        async move {
            let original_url = leader_url.read().clone();
            let mut url = original_url.clone();

            loop {
                let elapsed = Instant::now().duration_since(start);
                if elapsed >= request_timeout {
                    internal!(
                        "request timeout reached; last error: {}",
                        last_error_desc.unwrap_or_else(|| "(none)".into())
                    );
                }
                if url.is_none() {
                    let descriptor: ControllerDescriptor =
                        auth.get_leader().await.map_err(|e| {
                            internal_err(format!("failed to get current leader: {}", e))
                        })?;

                    url = Some(descriptor.controller_uri);
                }

                // FIXME(eta): error[E0277]: the trait bound `Uri: From<&Url>` is not satisfied
                //             (if you try and use the `url` directly instead of stringifying)
                #[allow(clippy::unwrap_used)]
                let string_url = url.as_ref().unwrap().join(path)?.to_string();
                let r = hyper::Request::post(string_url)
                    .body(hyper::Body::from(body.clone()))
                    .map_err(|e| internal_err(format!("http request failed: {}", e)))?;

                let res = match tokio::time::timeout(request_timeout - elapsed, client.request(r))
                    .await
                {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => {
                        last_error_desc = Some(e.to_string());
                        url = None;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(_) => {
                        internal!(
                            "request timeout reached; last error: {}",
                            last_error_desc.unwrap_or_else(|| "(none)".into())
                        );
                    }
                };

                let status = res.status();
                let body = hyper::body::to_bytes(res.into_body())
                    .await
                    .map_err(|he| internal_err(format!("hyper response failed: {}", he)))?;

                match status {
                    hyper::StatusCode::OK => {
                        // If all went well update the leader url if it changed.
                        if url != original_url {
                            *leader_url.write() = url;
                        }

                        return Ok(body);
                    }
                    hyper::StatusCode::INTERNAL_SERVER_ERROR => {
                        let err: ReadySetError = bincode::deserialize(&body)?;
                        return Err(err);
                    }
                    s => {
                        last_error_desc = Some(format!("got status {}", s));
                        if s == hyper::StatusCode::SERVICE_UNAVAILABLE {
                            url = None;
                        }

                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

/// A handle to a Noria controller.
///
/// This handle is the primary mechanism for interacting with a running Noria instance, and lets
/// you add and remove queries, retrieve handles for inserting or querying the underlying data, and
/// to perform meta-operations such as fetching the dataflow's GraphViz visualization.
///
/// To establish a new connection to Noria, use `ControllerHandle::new`, and pass in the
/// appropriate `Authority`.
///
/// Note that whatever Tokio Runtime you use to execute the `Future` that resolves into the
/// `ControllerHandle` will also be the one that executes all your reads and writes through `View`
/// and `Table`. Make sure that that `Runtime` stays alive, and continues to be driven, otherwise
/// none of your operations will ever complete! Furthermore, you *must* use the `Runtime` to
/// execute any futures returned from `ControllerHandle` (that is, you cannot just call `.wait()`
/// on them).
// TODO: this should be renamed to NoriaHandle, or maybe just Connection, since it also provides
// reads and writes, which aren't controller actions!
pub struct ControllerHandle {
    handle: Buffer<Controller, ControllerRequest>,
    domains: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    views: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    tracer: tracing::Dispatch,
    request_timeout: Option<Duration>,
    migration_timeout: Option<Duration>,
}

impl Clone for ControllerHandle {
    fn clone(&self) -> Self {
        ControllerHandle {
            handle: self.handle.clone(),
            domains: self.domains.clone(),
            views: self.views.clone(),
            tracer: self.tracer.clone(),
            request_timeout: self.request_timeout,
            migration_timeout: self.migration_timeout,
        }
    }
}

// this alias is needed to work around -> impl Trait capturing _all_ lifetimes by default
// the A parameter is needed so it gets captured into the impl Trait
type RpcFuture<'a, R: 'a> = impl Future<Output = ReadySetResult<R>> + 'a;

/// The size of the [`Buffer`][0] to use for requests to the [`ControllerHandle`].
///
/// Experimentation has shown that if this is set to 1, requests from a `clone()`d
/// [`ControllerHandle`] stall, but besides that we don't know much abbout what this value should be
/// set to. Number of cores, perhaps?
const CONTROLLER_BUFFER_SIZE: usize = 8;

impl ControllerHandle {
    #[doc(hidden)]
    pub fn make(
        authority: Arc<Authority>,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> Self {
        // need to use lazy otherwise current executor won't be known
        let tracer = tracing::dispatcher::get_default(|d| d.clone());
        let mut http_connector = HttpConnector::new();
        http_connector.set_connect_timeout(request_timeout);
        ControllerHandle {
            views: Default::default(),
            domains: Default::default(),
            handle: Buffer::new(
                Controller {
                    authority,
                    client: hyper::Client::builder()
                        .http2_only(true)
                        // Sets to the keep alive default if request_timeout is not specified.
                        .http2_keep_alive_timeout(
                            request_timeout.unwrap_or(Duration::from_secs(20)),
                        )
                        .build(http_connector),
                    leader_url: Arc::new(RwLock::new(None)),
                },
                CONTROLLER_BUFFER_SIZE,
            ),
            tracer,
            request_timeout,
            migration_timeout,
        }
    }

    /// Check that the `ControllerHandle` can accept another request.
    ///
    /// Note that this method _must_ return `Poll::Ready` before any other methods that return
    /// a `Future` on `ControllerHandle` can be called.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<ReadySetResult<()>> {
        self.handle
            .poll_ready(cx)
            .map_err(rpc_err!("ControllerHandle::poll_ready"))
    }

    /// A future that resolves when the controller can accept more messages.
    ///
    /// When this future resolves, you it is safe to call any methods that require `poll_ready` to
    /// have returned `Poll::Ready`.
    pub async fn ready(&mut self) -> ReadySetResult<()> {
        future::poll_fn(move |cx| self.poll_ready(cx)).await
    }

    /// Create a [`ControllerHandle`] that bootstraps a connection to Noria via the configuration
    /// stored in the given `authority`. Assigns the authority no timeouts for requests and
    /// migrations.
    pub async fn new<I: Into<Arc<Authority>>>(authority: I) -> Self {
        Self::make(authority.into(), None, None)
    }

    /// Create a [`ControllerHandle`] that bootstraps a connection to Noria via the configuration
    /// stored in the given `authority`. Assigns a timeout to requests to Noria, `timeout`.
    pub async fn with_timeouts<I: Into<Arc<Authority>>>(
        authority: I,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> Self {
        Self::make(authority.into(), request_timeout, migration_timeout)
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    pub async fn inputs(&mut self) -> ReadySetResult<BTreeMap<String, NodeIndex>> {
        let body: hyper::body::Bytes = self
            .handle
            .ready()
            .await
            .map_err(rpc_err!("ControllerHandle::inputs"))?
            .call(ControllerRequest::new("inputs", &(), None)?)
            .await
            .map_err(rpc_err!("ControllerHandle::inputs"))?;

        Ok(bincode::deserialize(&body)?)
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE EXT VIEW` statement in a recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub async fn outputs(&mut self) -> ReadySetResult<BTreeMap<String, NodeIndex>> {
        let body: hyper::body::Bytes = self
            .handle
            .ready()
            .await
            .map_err(rpc_err!("ControllerHandle::outputs"))?
            .call(ControllerRequest::new(
                "outputs",
                &(),
                self.request_timeout,
            )?)
            .await
            .map_err(rpc_err!("ControllerHandle::outputs"))?;

        Ok(bincode::deserialize(&body)?)
    }

    /// Enumerate all known external views. Includes the SqlQuery that created the view
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub async fn verbose_outputs(&mut self) -> ReadySetResult<BTreeMap<String, nom_sql::SqlQuery>> {
        let body: hyper::body::Bytes = self
            .handle
            .ready()
            .await
            .map_err(rpc_err!("ControllerHandle::verbose_outputs"))?
            .call(ControllerRequest::new(
                "verbose_outputs",
                &(),
                self.request_timeout,
            )?)
            .await
            .map_err(rpc_err!("ControllerHandle::verbose_outputs"))?;

        Ok(bincode::deserialize(&body)?)
    }

    /// Obtain a `View` that allows you to query the given external view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view<'a>(&'a mut self, name: &str) -> impl Future<Output = ReadySetResult<View>> + 'a {
        let name = name.to_string();
        let request = ViewRequest { name, filter: None };
        self.request_view(request)
    }

    /// Obtain a `View` from the given pool of workers, that allows you to query the given external
    /// view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view_from_workers<'a>(
        &'a mut self,
        name: &str,
        workers: Vec<Url>,
    ) -> impl Future<Output = ReadySetResult<View>> + 'a {
        let name = name.to_string();
        let request = ViewRequest {
            name,
            filter: Some(ViewFilter::Workers(workers)),
        };
        self.request_view(request)
    }

    /// Obtain a `View` from a named region, that allows you to query the given external view.
    /// If there is no worker with a replica in the region, a `View` from the first replica
    /// found is returned.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view_from_region<'a>(
        &'a mut self,
        name: &str,
        region: &str,
    ) -> impl Future<Output = ReadySetResult<View>> + 'a {
        let name = name.to_string();
        let request = ViewRequest {
            name,
            filter: Some(ViewFilter::Region(region.to_string())),
        };
        self.request_view(request)
    }

    /// Obtain a 'ViewBuilder' for a specific view that allows you to build a view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    /// This is made public for inspection in integration tests and is not meant to be
    /// used to construct views, instead use `view`, which calls this method.
    pub async fn view_builder(&mut self, view_request: ViewRequest) -> ReadySetResult<ViewBuilder> {
        let body: hyper::body::Bytes = self
            .handle
            .ready()
            .await
            .map_err(rpc_err!("ControllerHandle::view_builder"))?
            .call(ControllerRequest::new(
                "view_builder",
                &view_request,
                self.request_timeout,
            )?)
            .await
            .map_err(rpc_err!("ControllerHandle::view_builder"))?;

        match bincode::deserialize::<ReadySetResult<Option<ViewBuilder>>>(&body)?
            .map_err(|e| rpc_err_no_downcast("ControllerHandle::view_builder", e))?
        {
            Some(vb) => Ok(vb),
            None => match view_request.filter {
                Some(f) => match f {
                    ViewFilter::Workers(w) => Err(ReadySetError::ViewNotFoundInWorkers {
                        name: view_request.name,
                        workers: w,
                    }),
                    ViewFilter::Region(_) => Err(ReadySetError::ViewNotFound(view_request.name)),
                },
                None => Err(ReadySetError::ViewNotFound(view_request.name)),
            },
        }
    }

    fn request_view(
        &mut self,
        view_request: ViewRequest,
    ) -> impl Future<Output = ReadySetResult<View>> + '_ {
        let views = self.views.clone();
        async move {
            let region: Option<String> =
                if let Some(ViewFilter::Region(region)) = &view_request.filter {
                    Some(region.clone())
                } else {
                    None
                };
            let view_builder = self.view_builder(view_request).await?;
            view_builder.build(region, views)
        }
    }

    /// Obtain a `Table` that allows you to perform writes, deletes, and other operations on the
    /// given base table.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn table<'a>(&'a mut self, name: &str) -> impl Future<Output = ReadySetResult<Table>> + 'a {
        let domains = self.domains.clone();
        let name = name.to_string();
        async move {
            let body: hyper::body::Bytes = self
                .handle
                .ready()
                .await
                .map_err(rpc_err!("ControllerHandle::table"))?
                .call(ControllerRequest::new(
                    "table_builder",
                    &name,
                    self.request_timeout,
                )?)
                .await
                .map_err(rpc_err!("ControllerHandle::table"))?;

            match bincode::deserialize::<ReadySetResult<Option<TableBuilder>>>(&body)?
                .map_err(|e| rpc_err_no_downcast("ControllerHandle::table", e))?
            {
                Some(tb) => Ok(tb.build(domains)),
                None => Err(ReadySetError::TableNotFound(name)),
            }
        }
    }

    /// Perform a raw RPC request to the HTTP `path` provided, providing a request body `r`.
    #[doc(hidden)]
    pub fn rpc<'a, Q, R>(
        &'a mut self,
        path: &'static str,
        r: Q,
        timeout: Option<Duration>,
    ) -> RpcFuture<'a, R>
    where
        for<'de> R: Deserialize<'de>,
        R: Send + 'static,
        Q: Serialize,
    {
        // Needed b/c of https://github.com/rust-lang/rust/issues/65442
        async fn rpc_inner<R>(
            ch: &mut ControllerHandle,
            req: ControllerRequest,
            path: &'static str,
        ) -> ReadySetResult<R>
        where
            for<'de> R: Deserialize<'de>,
        {
            let body: hyper::body::Bytes = ch
                .handle
                .ready()
                .await
                .map_err(rpc_err!(path))?
                .call(req)
                .await
                .map_err(rpc_err!(path))?;

            bincode::deserialize::<R>(&body)
                .map_err(ReadySetError::from)
                .map_err(|e| rpc_err_no_downcast(path, e))
        }

        match ControllerRequest::new(path, r, timeout) {
            Ok(req) => Either::Left(rpc_inner(self, req, path)),
            Err(e) => Either::Right(std::future::ready(Err(e))),
        }
    }

    /// Get statistics about the time spent processing different parts of the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn statistics(&mut self) -> impl Future<Output = ReadySetResult<stats::GraphStats>> + '_ {
        self.rpc("get_statistics", (), self.request_timeout)
    }

    /// Flush all partial state, evicting all rows present.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn flush_partial(&mut self) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("flush_partial", (), self.request_timeout)
    }

    /// Performs a dry-run migration with the given set of queries.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn dry_run(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Output = ReadySetResult<ActivationResult>> + '_ {
        let request = RecipeSpec {
            recipe: recipe_addition,
            ..Default::default()
        };

        self.rpc("dry_run", request, self.migration_timeout)
    }

    /// Extend the existing recipe with the given set of queries.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Output = ReadySetResult<ActivationResult>> + '_ {
        let request = RecipeSpec {
            recipe: recipe_addition,
            ..Default::default()
        };

        self.rpc("extend_recipe", request, self.migration_timeout)
    }

    /// Extend the existing recipe with the given set of queries and don't require leader ready.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe_no_leader_ready(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Output = ReadySetResult<ActivationResult>> + '_ {
        let request = RecipeSpec {
            recipe: recipe_addition,
            require_leader_ready: false,
            ..Default::default()
        };

        self.rpc("extend_recipe", request, self.migration_timeout)
    }

    /// Extend the existing recipe with the given set of queries and an optional replication offset
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe_with_offset(
        &mut self,
        recipe_addition: &str,
        replication_offset: &ReplicationOffset,
        require_leader_ready: bool,
    ) -> impl Future<Output = ReadySetResult<ActivationResult>> + '_ {
        let request = RecipeSpec {
            recipe: recipe_addition,
            replication_offset: Some(Cow::Borrowed(replication_offset)),
            require_leader_ready,
        };

        self.rpc("extend_recipe", request, self.migration_timeout)
    }

    /// Replace the existing recipe with this one.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn install_recipe(
        &mut self,
        new_recipe: &str,
    ) -> impl Future<Output = ReadySetResult<ActivationResult>> + '_ {
        let request = RecipeSpec {
            recipe: new_recipe,
            ..Default::default()
        };

        self.rpc("install_recipe", request, self.migration_timeout)
    }

    /// Remove all nodes related to the query with the given name
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn remove_query(&mut self, name: &str) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("remove_query", name, self.migration_timeout)
    }

    /// Set the replication offset for the schema, which is stored with the recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn set_schema_replication_offset(
        &mut self,
        replication_offset: Option<&ReplicationOffset>,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc(
            "set_schema_replication_offset",
            replication_offset,
            self.request_timeout,
        )
    }

    /// Fetch a graphviz description of the dataflow graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn graphviz(&mut self) -> impl Future<Output = ReadySetResult<String>> + '_ {
        self.rpc("graphviz", (), self.request_timeout)
    }

    /// Fetch a simplified graphviz description of the dataflow graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn simple_graphviz(&mut self) -> impl Future<Output = ReadySetResult<String>> + '_ {
        self.rpc("simple_graphviz", (), self.request_timeout)
    }

    /// Replicate the readers associated with the list of queries to the given worker.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn get_instances(&mut self) -> impl Future<Output = ReadySetResult<Vec<(Url, bool)>>> + '_ {
        self.rpc("instances", (), self.request_timeout)
    }

    /// Replicate the readers associated with the list of queries to the given worker.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn replicate_readers(
        &mut self,
        queries: Vec<String>,
        worker_uri: Option<Url>,
    ) -> impl Future<Output = ReadySetResult<ReaderReplicationResult>> + '_ {
        let request = ReaderReplicationSpec {
            queries,
            worker_uri,
        };
        self.rpc("replicate_readers", request, self.migration_timeout)
    }

    /// Query the controller for information about the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn get_info(&mut self) -> impl Future<Output = ReadySetResult<GraphInfo>> + '_ {
        self.rpc("get_info", (), self.request_timeout)
    }

    /// Remove the given external view from the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn remove_node(
        &mut self,
        view: NodeIndex,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", view, self.migration_timeout)
    }

    /// Fetch a dump of metrics values from the running noria instance
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn metrics_dump(&mut self) -> impl Future<Output = ReadySetResult<MetricsDump>> + '_ {
        self.rpc("metrics_dump", (), self.request_timeout)
    }

    /// Get a list of all registered worker URIs.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn workers(&mut self) -> impl Future<Output = ReadySetResult<Vec<Url>>> + '_ {
        self.rpc("workers", (), self.request_timeout)
    }

    /// Get a list of all registered worker URIs that are currently healthy.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn healthy_workers(&mut self) -> impl Future<Output = ReadySetResult<Vec<Url>>> + '_ {
        self.rpc("healthy_workers", (), self.request_timeout)
    }

    /// Get the url of the current noria controller.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn controller_uri(&mut self) -> impl Future<Output = ReadySetResult<Url>> + '_ {
        self.rpc("controller_uri", (), self.request_timeout)
    }

    /// Get a set of all replication offsets for the entire system.
    ///
    /// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    pub fn replication_offsets(
        &mut self,
    ) -> impl Future<Output = ReadySetResult<ReplicationOffsets>> + '_ {
        self.rpc("replication_offsets", (), self.request_timeout)
    }

    /// Get a list of all current tables node indexes that are involved in snapshotting.
    pub fn snapshotting_tables(
        &mut self,
    ) -> impl Future<Output = ReadySetResult<Vec<String>>> + '_ {
        self.rpc("snapshotting_tables", (), self.request_timeout)
    }

    /// Return whether the leader is ready or not.
    pub fn leader_ready(&mut self) -> impl Future<Output = ReadySetResult<bool>> + '_ {
        self.rpc("leader_ready", (), self.request_timeout)
    }

    /// Returns the ReadySetStatus struct returned by the leader.
    pub fn status(&mut self) -> impl Future<Output = ReadySetResult<ReadySetStatus>> + '_ {
        self.rpc("status", (), self.request_timeout)
    }

    #[cfg(feature = "failure_injection")]
    pub fn failpoint(
        &mut self,
        name: String,
        action: String,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("failpoint", (name, action), self.request_timeout)
    }
}
