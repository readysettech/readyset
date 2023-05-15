use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::future;
use hyper::client::HttpConnector;
use nom_sql::{Relation, SelectStatement};
use parking_lot::RwLock;
use petgraph::graph::NodeIndex;
use readyset_errors::{
    internal, internal_err, rpc_err, rpc_err_no_downcast, ReadySetError, ReadySetResult,
};
use serde::de::DeserializeOwned;
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
use crate::recipe::changelist::ChangeList;
use crate::recipe::{ExtendRecipeResult, ExtendRecipeSpec, MigrationStatus};
use crate::replication::ReplicationOffsets;
use crate::status::ReadySetStatus;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::{NodeSize, ReplicationOffset, TableStatus, ViewCreateRequest, ViewFilter, ViewRequest};

mod rpc;

const EXTEND_RECIPE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Describes a running controller instance.
///
/// A serialized version of this struct is stored in ZooKeeper so that clients can reach the
/// currently active controller.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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
                    let descriptor: ControllerDescriptor = auth
                        .get_leader()
                        .await
                        .map_err(|e| internal_err!("failed to get current leader: {}", e))?;

                    url = Some(descriptor.controller_uri);
                }

                // FIXME(eta): error[E0277]: the trait bound `Uri: From<&Url>` is not satisfied
                //             (if you try and use the `url` directly instead of stringifying)
                #[allow(clippy::unwrap_used)]
                let string_url = url.as_ref().unwrap().join(path)?.to_string();
                let r = hyper::Request::post(string_url)
                    .body(hyper::Body::from(body.clone()))
                    .map_err(|e| internal_err!("http request failed: {}", e))?;

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
                    .map_err(|he| internal_err!("hyper response failed: {}", he))?;

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

/// A handle to a ReadySet controller.
///
/// This handle is the primary mechanism for interacting with a running ReadySet instance, and lets
/// you add and remove queries, retrieve handles for inserting or querying the underlying data, and
/// to perform meta-operations such as fetching the dataflow's GraphViz visualization.
///
/// To establish a new connection to ReadySet, use `ReadySetHandle::new`, and pass in the
/// appropriate `Authority`.
///
/// Note that whatever Tokio Runtime you use to execute the `Future` that resolves into the
/// `ReadySetHandle` will also be the one that executes all your reads and writes through `View`
/// and `Table`. Make sure that that `Runtime` stays alive, and continues to be driven, otherwise
/// none of your operations will ever complete! Furthermore, you *must* use the `Runtime` to
/// execute any futures returned from `ReadySetHandle` (that is, you cannot just call `.wait()`
/// on them).
pub struct ReadySetHandle {
    handle: Buffer<Controller, ControllerRequest>,
    domains: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    views: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    tracer: tracing::Dispatch,
    request_timeout: Option<Duration>,
    migration_timeout: Option<Duration>,
}

impl Clone for ReadySetHandle {
    fn clone(&self) -> Self {
        ReadySetHandle {
            handle: self.handle.clone(),
            domains: self.domains.clone(),
            views: self.views.clone(),
            tracer: self.tracer.clone(),
            request_timeout: self.request_timeout,
            migration_timeout: self.migration_timeout,
        }
    }
}

/// The size of the [`Buffer`][0] to use for requests to the [`ReadySetHandle`].
///
/// Experimentation has shown that if this is set to 1, requests from a `clone()`d
/// [`ReadySetHandle`] stall, but besides that we don't know much abbout what this value should be
/// set to. Number of cores, perhaps?
const CONTROLLER_BUFFER_SIZE: usize = 8;

impl ReadySetHandle {
    pub fn make(
        authority: Arc<Authority>,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> Self {
        // need to use lazy otherwise current executor won't be known
        let tracer = tracing::dispatcher::get_default(|d| d.clone());
        let mut http_connector = HttpConnector::new();
        http_connector.set_connect_timeout(request_timeout);
        ReadySetHandle {
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

    /// Check that the `ReadySetHandle` can accept another request.
    ///
    /// Note that this method _must_ return `Poll::Ready` before any other methods that return
    /// a `Future` on `ReadySetHandle` can be called.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<ReadySetResult<()>> {
        self.handle
            .poll_ready(cx)
            .map_err(rpc_err!("ReadySetHandle::poll_ready"))
    }

    /// A future that resolves when the controller can accept more messages.
    ///
    /// When this future resolves, you it is safe to call any methods that require `poll_ready` to
    /// have returned `Poll::Ready`.
    pub async fn ready(&mut self) -> ReadySetResult<()> {
        future::poll_fn(move |cx| self.poll_ready(cx)).await
    }

    /// Create a [`ReadySetHandle`] that bootstraps a connection to ReadySet via the configuration
    /// stored in the given `authority`. Assigns the authority no timeouts for requests and
    /// migrations.
    pub async fn new<I: Into<Arc<Authority>>>(authority: I) -> Self {
        Self::make(authority.into(), None, None)
    }

    /// Create a [`ReadySetHandle`] that bootstraps a connection to ReadySet via the configuration
    /// stored in the given `authority`. Assigns a timeout to requests to ReadySet, `timeout`.
    pub async fn with_timeouts<I: Into<Arc<Authority>>>(
        authority: I,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> Self {
        Self::make(authority.into(), request_timeout, migration_timeout)
    }

    async fn simple_get_request<R>(&mut self, path: &'static str) -> ReadySetResult<R>
    where
        R: DeserializeOwned,
    {
        let body: hyper::body::Bytes = self
            .handle
            .ready()
            .await
            .map_err(rpc_err!(format_args!("ReadySetHandle::{}", path)))?
            .call(ControllerRequest::new(path, (), None)?)
            .await
            .map_err(rpc_err!(format_args!("ReadySetHandle::{}", path)))?;

        bincode::deserialize(&body)
            .map_err(ReadySetError::from)
            .map_err(Box::new)
            .map_err(rpc_err!(format_args!("ReadySetHandle::{}", path)))
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    pub async fn tables(&mut self) -> ReadySetResult<BTreeMap<Relation, NodeIndex>> {
        self.simple_get_request("tables").await
    }

    /// Query the status of all known tables, including those not replicated by ReadySet
    pub async fn table_statuses(&mut self) -> ReadySetResult<BTreeMap<Relation, TableStatus>> {
        self.simple_get_request("table_statuses").await?
    }

    /// Return a list of all relations (tables or views) which are known to exist in the upstream
    /// database that we are replicating from, but are not being replicated to ReadySet (which are
    /// recorded via [`Change::AddNonReplicatedRelation`]).
    ///
    /// [`Change::AddNonReplicatedRelation`]: readyset_client::recipe::changelist::Change::AddNonReplicatedRelation
    pub async fn non_replicated_relations(&mut self) -> ReadySetResult<HashSet<Relation>> {
        self.simple_get_request("non_replicated_relations").await
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE CACHE` or `CREATE VIEW` statement in a
    /// recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub async fn views(&mut self) -> ReadySetResult<BTreeMap<Relation, NodeIndex>> {
        self.simple_get_request("views").await
    }

    /// Enumerate all known external views. Includes the SqlQuery that created the view
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub async fn verbose_views(
        &mut self,
    ) -> ReadySetResult<BTreeMap<Relation, (SelectStatement, bool)>> {
        self.simple_get_request("verbose_views").await
    }

    /// For each of the given list of queries, determine whether that query (or a semantically
    /// equivalent query) has been created as a `View`.
    ///
    /// To save on data, this returns a list of booleans corresponding to the provided list of
    /// query, where each boolean is `true` if the query at the same position in the argument list
    /// has been installed as a view.
    pub async fn view_statuses(
        &mut self,
        queries: Vec<ViewCreateRequest>,
        dialect: dataflow_expression::Dialect,
    ) -> ReadySetResult<Vec<bool>> {
        self.rpc("view_statuses", (queries, dialect), self.request_timeout)
            .await
    }

    /// Obtain a `View` that allows you to query the given external view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view<I: Into<Relation>>(
        &mut self,
        name: I,
    ) -> impl Future<Output = ReadySetResult<View>> + '_ {
        let request = ViewRequest {
            name: name.into(),
            filter: None,
        };
        self.request_view(request)
    }

    /// Obtain a `View` from the given pool of workers, that allows you to query the given external
    /// view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view_from_workers<I: Into<Relation>>(
        &mut self,
        name: I,
        workers: Vec<Url>,
    ) -> impl Future<Output = ReadySetResult<View>> + '_ {
        let request = ViewRequest {
            name: name.into(),
            filter: Some(ViewFilter::Workers(workers)),
        };
        self.request_view(request)
    }

    /// Obtain the replica of a `View` with the given replica index
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view_with_replica<I: Into<Relation>>(
        &mut self,
        name: I,
        replica: usize,
    ) -> impl Future<Output = ReadySetResult<View>> + '_ {
        self.request_view(ViewRequest {
            name: name.into(),
            filter: Some(ViewFilter::Replica(replica)),
        })
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
            .map_err(rpc_err!("ReadySetHandle::view_builder"))?
            .call(ControllerRequest::new(
                "view_builder",
                &view_request,
                self.request_timeout,
            )?)
            .await
            .map_err(rpc_err!("ReadySetHandle::view_builder"))?;

        match bincode::deserialize::<ReadySetResult<Option<ViewBuilder>>>(&body)?
            .map_err(|e| rpc_err_no_downcast("ReadySetHandle::view_builder", e))?
        {
            Some(vb) => Ok(vb),
            None => match view_request.filter {
                Some(ViewFilter::Workers(w)) => Err(ReadySetError::ViewNotFoundInWorkers {
                    name: view_request.name.display_unquoted().to_string(),
                    workers: w,
                }),
                _ => Err(ReadySetError::ViewNotFound(
                    view_request.name.display_unquoted().to_string(),
                )),
            },
        }
    }

    fn request_view(
        &mut self,
        view_request: ViewRequest,
    ) -> impl Future<Output = ReadySetResult<View>> + '_ {
        let views = self.views.clone();
        async move {
            let replica = if let Some(ViewFilter::Replica(replica)) = &view_request.filter {
                Some(*replica)
            } else {
                None
            };
            let view_builder = self.view_builder(view_request).await?;
            view_builder.build(replica, views)
        }
    }

    /// Obtain a `Table` that allows you to perform writes, deletes, and other operations on the
    /// given base table, looking up the table by its name
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn table<N>(&mut self, name: N) -> impl Future<Output = ReadySetResult<Table>> + '_
    where
        N: Into<Relation>,
    {
        let name = name.into();
        async move {
            self.request_table(ControllerRequest::new(
                "table_builder",
                &name,
                self.request_timeout,
            )?)
            .await?
            .ok_or_else(|| ReadySetError::TableNotFound {
                name: name.name.clone().into(),
                schema: name.schema.clone().map(Into::into),
            })
        }
    }

    /// Obtain a `Table` that allows you to perform writes, deletes, and other operations on the
    /// given base table, looking up the table by its node index
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub async fn table_by_index(&mut self, ni: NodeIndex) -> ReadySetResult<Table> {
        self.request_table(ControllerRequest::new(
            "table_builder_by_index",
            ni,
            self.request_timeout,
        )?)
        .await?
        .ok_or_else(|| ReadySetError::NoSuchNode(ni.index()))
    }

    fn request_table(
        &mut self,
        req: ControllerRequest,
    ) -> impl Future<Output = ReadySetResult<Option<Table>>> + '_ {
        let domains = self.domains.clone();
        async move {
            let body: hyper::body::Bytes = self
                .handle
                .ready()
                .await
                .map_err(rpc_err!("ReadySetHandle::table"))?
                .call(req)
                .await
                .map_err(rpc_err!("ReadySetHandle::table"))?;

            Ok(
                bincode::deserialize::<ReadySetResult<Option<TableBuilder>>>(&body)?
                    .map_err(|e| rpc_err_no_downcast("ReadySetHandle::table", e))?
                    .map(|tb| tb.build(domains)),
            )
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
        changes: ChangeList,
    ) -> impl Future<Output = ReadySetResult<ExtendRecipeResult>> + '_ {
        let request = ExtendRecipeSpec::from(changes);

        self.rpc("dry_run", request, self.migration_timeout)
    }

    /// Extend the existing recipe with the given set of queries.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe(
        &mut self,
        changes: ChangeList,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        let request = ExtendRecipeSpec::from(changes);

        async move {
            match self
                .rpc("extend_recipe", request, self.migration_timeout)
                .await?
            {
                ExtendRecipeResult::Done => Ok(()),
                ExtendRecipeResult::Pending(migration_id) => {
                    while self
                        .rpc::<_, MigrationStatus>(
                            "migration_status",
                            migration_id,
                            self.migration_timeout,
                        )
                        .await?
                        .is_pending()
                    {
                        tokio::time::sleep(EXTEND_RECIPE_POLL_INTERVAL).await;
                    }
                    Ok(())
                }
            }
        }
    }

    /// Extend the existing recipe with the given set of queries and don't require leader ready.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe_no_leader_ready(
        &mut self,
        changes: ChangeList,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        let request = ExtendRecipeSpec {
            require_leader_ready: false,
            ..changes.into()
        };

        self.rpc("extend_recipe", request, self.migration_timeout)
    }

    /// Extend the existing recipe with the given set of queries and an optional replication offset
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe_with_offset(
        &mut self,
        changes: ChangeList,
        replication_offset: &ReplicationOffset,
        require_leader_ready: bool,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        let request = ExtendRecipeSpec {
            changes,
            replication_offset: Some(Cow::Borrowed(replication_offset)),
            require_leader_ready,
        };

        self.rpc("extend_recipe", request, self.migration_timeout)
    }

    /// Remove all nodes related to the query with the given name
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn remove_query(
        &mut self,
        name: &Relation,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("remove_query", name, self.migration_timeout)
    }

    /// Remove all non-base nodes from the graph
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn remove_all_queries(&mut self) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("remove_all_queries", (), self.migration_timeout)
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
    /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
    /// for more information about replication offsets.
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

    /// Return a map of node indices to key counts.
    pub fn node_sizes(
        &mut self,
    ) -> impl Future<Output = ReadySetResult<HashMap<NodeIndex, NodeSize>>> + '_ {
        self.rpc("node_sizes", (), self.request_timeout)
    }

    /// Return whether the leader is ready or not.
    pub fn leader_ready(&mut self) -> impl Future<Output = ReadySetResult<bool>> + '_ {
        self.rpc("leader_ready", (), self.request_timeout)
    }

    /// Returns the ReadySetStatus struct returned by the leader.
    pub fn status(&mut self) -> impl Future<Output = ReadySetResult<ReadySetStatus>> + '_ {
        self.rpc("status", (), self.request_timeout)
    }

    /// Returns true if topk and pagination support are enabled on the server
    pub fn supports_pagination(&mut self) -> impl Future<Output = ReadySetResult<bool>> + '_ {
        self.rpc("supports_pagination", (), self.request_timeout)
    }

    /// Returns the server's release version
    pub fn version(&mut self) -> impl Future<Output = ReadySetResult<String>> + '_ {
        self.rpc("version", (), self.request_timeout)
    }

    /// Returns the amount of actually allocated memory
    pub fn allocated_bytes(&mut self) -> impl Future<Output = ReadySetResult<Option<usize>>> + '_ {
        self.rpc("allocated_bytes", (), self.request_timeout)
    }

    /// Set memory limit parameters
    pub fn set_memory_limit(
        &mut self,
        period: Option<Duration>,
        limit: Option<usize>,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("set_memory_limit", (period, limit), self.request_timeout)
    }

    #[cfg(feature = "failure_injection")]
    /// Set a failpoint with provided name and action
    pub fn failpoint(
        &mut self,
        name: String,
        action: String,
    ) -> impl Future<Output = ReadySetResult<()>> + '_ {
        self.rpc("failpoint", (name, action), self.request_timeout)
    }
}
