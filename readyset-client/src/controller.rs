use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::future;
use hyper::client::HttpConnector;
use nom_sql::{CreateCacheStatement, NonReplicatedRelation, Relation};
use parking_lot::RwLock;
use petgraph::graph::NodeIndex;
use readyset_errors::{
    internal, internal_err, rpc_err, rpc_err_no_downcast, ReadySetError, ReadySetResult,
};
use replication_offset::ReplicationOffsets;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower::ServiceExt;
use tower_service::Service;
use tracing::{debug, trace};
use url::Url;

use crate::consensus::{Authority, AuthorityControl};
use crate::debug::info::{GraphInfo, MaterializationInfo, NodeSize};
use crate::debug::stats;
use crate::internal::{DomainIndex, ReplicaAddress};
use crate::metrics::MetricsDump;
use crate::recipe::changelist::ChangeList;
use crate::recipe::{ExtendRecipeResult, ExtendRecipeSpec, MigrationStatus};
use crate::status::ReadySetStatus;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::{
    ReplicationOffset, SingleKeyEviction, TableStatus, ViewCreateRequest, ViewFilter, ViewRequest,
};

mod rpc;

const EXTEND_RECIPE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const WAIT_FOR_ALL_TABLES_TO_COMPACT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Describes a running controller instance.
///
/// A serialized version of this struct is stored in the Authority so that clients can reach the
/// currently active controller.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ControllerDescriptor {
    pub controller_uri: Url,
    pub nonce: u64,
}

fn make_http_client(timeout: Option<Duration>) -> hyper::Client<hyper::client::HttpConnector> {
    let mut http_connector = HttpConnector::new();
    http_connector.set_connect_timeout(timeout);
    hyper::Client::builder()
        .http2_only(true)
        // Sets to the keep alive default if request_timeout is not specified.
        .http2_keep_alive_timeout(timeout.unwrap_or(Duration::from_secs(20)))
        .build(http_connector)
}

/// Errors that can occur when making a request to a controller
struct ControllerRequestError {
    /// The error itself
    error: ReadySetError,
    /// Does this error mean that the controller URL used to make the request is invalid, and a new
    /// URL should be retrieved from the authority
    invalidate_url: bool,
    /// Is this error permanent, meaning the request should not be retried?
    permanent: bool,
}

impl<E> From<E> for ControllerRequestError
where
    ReadySetError: From<E>,
{
    fn from(error: E) -> Self {
        Self {
            error: error.into(),
            invalidate_url: false,
            permanent: true,
        }
    }
}

async fn controller_request(
    url: &Url,
    client: &hyper::Client<hyper::client::HttpConnector>,
    req: ControllerRequest,
    timeout: Duration,
) -> Result<hyper::body::Bytes, ControllerRequestError> {
    // FIXME(eta): error[E0277]: the trait bound `Uri: From<&Url>` is not satisfied
    //             (if you try and use the `url` directly instead of stringifying)
    #[allow(clippy::unwrap_used)]
    let string_url = url.join(req.path)?.to_string();

    let r = hyper::Request::post(string_url)
        .body(hyper::Body::from(req.request.clone()))
        .map_err(|e| internal_err!("http request failed: {}", e))?;

    let res = match tokio::time::timeout(timeout, client.request(r)).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            return Err(ControllerRequestError {
                error: e.into(),
                invalidate_url: true,
                permanent: false,
            })
        }
        Err(_) => {
            internal!("request timeout reached!");
        }
    };

    let status = res.status();
    let body = hyper::body::to_bytes(res.into_body())
        .await
        .map_err(|he| internal_err!("hyper response failed: {}", he))?;

    match status {
        hyper::StatusCode::OK => Ok(body),
        hyper::StatusCode::INTERNAL_SERVER_ERROR => {
            let err: ReadySetError = bincode::deserialize(&body)?;
            Err(err.into())
        }
        s => Err(ControllerRequestError {
            error: internal_err!("HTTP status {s}"),
            invalidate_url: s == hyper::StatusCode::SERVICE_UNAVAILABLE,
            permanent: false,
        }),
    }
}

/// A direct handle to a controller instance running at a known URL, without access to an authority
#[derive(Clone)]
struct RawController {
    url: Url,
    client: hyper::Client<hyper::client::HttpConnector>,
    request_timeout: Option<Duration>,
}

impl RawController {
    /// Create a new handle to a controller instance with a known URL, optionally configured to use
    /// the given timeout for requests
    fn new(url: Url, request_timeout: Option<Duration>) -> Self {
        Self {
            url,
            client: make_http_client(request_timeout),
            request_timeout,
        }
    }
}

impl Service<ControllerRequest> for RawController {
    type Response = hyper::body::Bytes;
    type Error = ReadySetError;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ControllerRequest) -> Self::Future {
        trace!(?req, "Issuing controller RPC");
        let url = self.url.clone();
        let client = self.client.clone();
        let request_timeout = self.request_timeout.unwrap_or(Duration::MAX);
        async move {
            controller_request(&url, &client, req, request_timeout)
                .await
                .map_err(|e| e.error)
        }
    }
}

#[derive(Clone)]
struct Controller {
    authority: Arc<Authority>,
    client: hyper::Client<hyper::client::HttpConnector>,
    /// The last valid leader URL seen by this service. Used to circumvent requests to Consul in
    /// the happy-path.
    leader_url: Arc<RwLock<Option<Url>>>,
}

#[derive(Debug, Clone)]
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

                let url_ = match &url {
                    Some(u) => u,
                    None => {
                        let descriptor = match tokio::time::timeout(
                            request_timeout - elapsed,
                            auth.get_leader(),
                        )
                        .await
                        {
                            Ok(Ok(url)) => url,
                            Ok(Err(_)) | Err(_) => {
                                return Err(ReadySetError::ControllerUnavailable);
                            }
                        };

                        url.insert(descriptor.controller_uri)
                    }
                };

                match controller_request(url_, &client, req.clone(), request_timeout - elapsed)
                    .await
                {
                    Ok(res) => {
                        if url != original_url {
                            *leader_url.write() = url;
                        }
                        return Ok(res);
                    }
                    Err(ControllerRequestError {
                        error,
                        invalidate_url,
                        permanent,
                    }) => {
                        if permanent {
                            return Err(error);
                        }

                        last_error_desc = Some(error.to_string());
                        if invalidate_url {
                            url = None
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

/// Options for generating graphviz [dot][] visualizations of the ReadySet dataflow graph.
///
/// Used as the argument to [`ReadySetHandle::graphviz`].
///
/// [dot]: https://graphviz.org/doc/info/lang.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphvizOptions {
    /// Limit to only visualizing the graph for a single query by name
    pub for_query: Option<Relation>,
    /// Generate a detailed representation of the graph, larger and with more information
    pub detailed: bool,
}

impl Default for GraphvizOptions {
    fn default() -> Self {
        Self {
            for_query: None,
            detailed: true,
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
#[derive(Clone)]
pub struct ReadySetHandle {
    handle: tower::util::Either<Controller, RawController>,
    domains: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    views: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    request_timeout: Option<Duration>,
    migration_timeout: Option<Duration>,
}

/// Define a simple RPC request wrapper for the controller, which queries the same RPC endpoint as
/// the name of the function and takes no arguments.
///
/// This is a common enough pattern that it's worth defining a macro for, but anything more complex
/// than this is worth defining as a regular function
macro_rules! simple_request {
    ($(#[$($attrss:tt)*])* $name: ident()) => { simple_request!($name() -> ()); };
    ($(#[$($attrss:tt)*])* $name: ident() -> $ret: ty) => {
        $(#[$($attrss)*])*
        pub fn $name(&mut self) -> impl Future<Output = ReadySetResult<$ret>> + '_ {
            self.rpc(stringify!($name), (), self.request_timeout)
        }
    };
    ($(#[$($attrss:tt)*])* $name: ident($arg: ident : $ty: ty) -> $ret: ty) => {
        $(#[$($attrss)*])*
        pub fn $name(&mut self, $arg : $ty) -> impl Future<Output = ReadySetResult<$ret>> + '_ {
            self.rpc(stringify!($name), $arg, self.request_timeout)
        }
    };
    ($(#[$($attrss:tt)*])* $name: ident($($arg: ident : $ty: ty,)+) -> $ret: ty) => {
        $(#[$($attrss)*])*
        pub fn $name(&mut self, $($arg : $ty,)+) -> impl Future<Output = ReadySetResult<$ret>> + '_ {
            self.rpc(stringify!($name), ($($arg,)+), self.request_timeout)
        }
    };
}

impl ReadySetHandle {
    pub fn make(
        authority: Arc<Authority>,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> Self {
        ReadySetHandle {
            views: Default::default(),
            domains: Default::default(),
            handle: tower::util::Either::A(Controller {
                authority,
                client: make_http_client(request_timeout),
                leader_url: Arc::new(RwLock::new(None)),
            }),
            request_timeout,
            migration_timeout,
        }
    }

    /// Make a new controller handle for connecting directly to a controller running at the given
    /// URL, without access to an authority
    pub fn make_raw(
        url: Url,
        request_timeout: Option<Duration>,
        migration_timeout: Option<Duration>,
    ) -> Self {
        ReadySetHandle {
            views: Default::default(),
            domains: Default::default(),
            handle: tower::util::Either::B(RawController::new(url, request_timeout)),
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

    /// Issues a POST request to the given path with no body.
    async fn simple_post_request<R>(&mut self, path: &'static str) -> ReadySetResult<R>
    where
        R: DeserializeOwned,
    {
        let body: hyper::body::Bytes = self
            .handle
            .ready()
            .await
            .map_err(rpc_err!(format_args!("ReadySetHandle::{}", path)))?
            .call(ControllerRequest::new(path, (), self.request_timeout)?)
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
        self.simple_post_request("tables").await
    }

    /// Query the status of all known tables, including those not replicated by ReadySet
    pub async fn table_statuses(&mut self) -> ReadySetResult<BTreeMap<Relation, TableStatus>> {
        self.simple_post_request("table_statuses").await?
    }

    /// Return a list of all relations (tables or views) which are known to exist in the upstream
    /// database that we are replicating from, but are not being replicated to ReadySet (which are
    /// recorded via [`Change::AddNonReplicatedRelation`]).
    ///
    /// [`Change::AddNonReplicatedRelation`]: readyset_client::recipe::changelist::Change::AddNonReplicatedRelation
    pub async fn non_replicated_relations(
        &mut self,
    ) -> ReadySetResult<HashSet<NonReplicatedRelation>> {
        self.simple_post_request("non_replicated_relations").await
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE CACHE` or `CREATE VIEW` statement in a
    /// recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub async fn views(&mut self) -> ReadySetResult<BTreeMap<Relation, NodeIndex>> {
        self.simple_post_request("views").await
    }

    /// Enumerate all known external views. Includes the SqlQuery that created
    /// the view and the fallback behavior.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call
    /// this method.
    pub async fn verbose_views(&mut self) -> ReadySetResult<Vec<CreateCacheStatement>> {
        self.simple_post_request("verbose_views").await
    }

    simple_request!(
        /// For each of the given list of queries, determine whether that query (or a semantically
        /// equivalent query) has been created as a `View`.
        ///
        /// To save on data, this returns a list of booleans corresponding to the provided list of
        /// query, where each boolean is `true` if the query at the same position in the argument list
        /// has been installed as a view.
        view_statuses(
            queries: Vec<ViewCreateRequest>,
            dialect: dataflow_expression::Dialect,
        ) -> Vec<bool>
    );

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
            view_builder.build(replica, views).await
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

            match bincode::deserialize::<ReadySetResult<Option<TableBuilder>>>(&body)?
                .map_err(|e| rpc_err_no_downcast("ReadySetHandle::table", e))?
            {
                Some(tb) => Ok(Some(tb.build(domains).await)),
                None => Ok(None),
            }
        }
    }

    simple_request!(
        /// Get statistics about the time spent processing different parts of the graph.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        statistics() -> stats::GraphStats
    );

    simple_request!(
        /// Flush all partial state, evicting all rows present.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        flush_partial()
    );

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
                    while self.migration_status(migration_id).await?.is_pending() {
                        tokio::time::sleep(EXTEND_RECIPE_POLL_INTERVAL).await;
                    }
                    Ok(())
                }
            }
        }
    }

    /// Query the status of a pending migration identified by the given `migration_id`.
    pub fn migration_status(
        &mut self,
        migration_id: u64,
    ) -> impl Future<Output = ReadySetResult<MigrationStatus>> + '_ {
        self.rpc::<_, MigrationStatus>("migration_status", migration_id, self.request_timeout)
    }

    /// Asynchronous version of extend_recipe(). The Controller should immediately return an ID that
    /// can be used to query the migration status.
    pub fn extend_recipe_async(
        &mut self,
        changes: ChangeList,
    ) -> impl Future<Output = ReadySetResult<u64>> + '_ {
        let request = ExtendRecipeSpec::from(changes).concurrently();
        async move {
            match self
                .rpc("extend_recipe", request, self.migration_timeout)
                .await?
            {
                ExtendRecipeResult::Pending(migration_id) => Ok(migration_id),
                ExtendRecipeResult::Done => {
                    internal!("CREATE CACHE CONCURRENTLY did not return migration id")
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
            concurrently: false,
        };

        self.rpc("extend_recipe", request, self.migration_timeout)
    }

    simple_request!(
        /// Remove all nodes related to the query with the given name
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        remove_query(name: &Relation) -> u64
    );

    simple_request!(
        /// Remove all non-base nodes from the graph
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        remove_all_queries()
    );

    simple_request!(
        /// Set the replication offset for the schema, which is stored with the recipe.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        set_schema_replication_offset(
            replication_offset: Option<&ReplicationOffset>,
        ) -> ()
    );

    simple_request!(
        /// Fetch a graphviz description of the dataflow graph.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        graphviz(options: GraphvizOptions) -> String
    );

    simple_request!(
        /// Fetch a simplified graphviz description of the dataflow graph.
        ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        simple_graphviz() -> String
    );

    simple_request!(
        /// Replicate the readers associated with the list of queries to the given worker.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        get_instances() -> Vec<(Url, bool)>
    );

    simple_request!(
        /// Query the controller for information about the graph.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        get_info() -> GraphInfo
    );

    simple_request!(
        /// Remove the given external view from the graph.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        remove_node(
            // TODO: this should likely take a view name, and we should verify that it's a Reader.
            view: NodeIndex,
        ) -> ()
    );

    simple_request!(
        /// Fetch a dump of metrics values from the running noria instance
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        metrics_dump() -> MetricsDump
    );

    simple_request!(
        /// Get a list of all registered worker URIs.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        workers() -> Vec<Url>
    );

    simple_request!(
        /// Get a list of all registered worker URIs that are currently healthy.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        healthy_workers() -> Vec<Url>
    );

    simple_request!(
        /// Get a map from domain index to a shard->replica mapping of the workers that are running the
        /// shard replicas of that domain .
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        domains() -> HashMap<DomainIndex, Vec<Vec<Option<Url>>>>
    );

    simple_request!(
        /// Get information about all materializations (stateful nodes) within the graph
        materialization_info() -> Vec<MaterializationInfo>
    );

    simple_request!(
        /// Get the url of the current noria controller.
        ///
        /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
        controller_uri() -> Url
    );

    simple_request!(
        /// Get a set of all replication offsets for the entire system.
        ///
        /// See [the documentation for
        /// PersistentState](::readyset_dataflow::state::persistent_state) for more information
        /// about replication offsets.
        replication_offsets() -> ReplicationOffsets
    );

    simple_request!(
        /// Gets the minimum replication offset up to which data has been persisted across all the base
        /// table nodes. If no base tables have unpersisted data, this method returns `None`.
        ///
        /// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state)
        /// for more information about replication offsets.
        min_persisted_replication_offset() -> Option<ReplicationOffset>
    );

    /// Poll in a loop to wait for all tables to finish compacting
    pub async fn wait_for_all_tables_to_compact(&mut self) -> ReadySetResult<()> {
        while !self
            .rpc("all_tables_compacted", (), self.request_timeout)
            .await?
        {
            debug!("Waiting for all tables to compact");
            tokio::time::sleep(WAIT_FOR_ALL_TABLES_TO_COMPACT_POLL_INTERVAL).await;
        }

        Ok(())
    }

    simple_request!(
        /// Return a map of node indices to key counts.
        node_sizes() -> HashMap<NodeIndex, NodeSize>
    );

    simple_request!(
        /// Return whether the leader is ready or not.
        leader_ready() -> bool
    );

    simple_request!(
        /// Returns the ReadySetStatus struct returned by the leader.
        status() -> ReadySetStatus
    );

    simple_request!(
        /// Returns true if topk and pagination support are enabled on the server
        supports_pagination() -> bool
    );

    simple_request!(
        /// Returns the server's release version
        version() -> String
    );

    simple_request!(
        /// Returns the amount of actually allocated memory
        allocated_bytes() -> Option<usize>
    );

    simple_request!(
        /// Set memory limit parameters
        set_memory_limit(
            period: Option<Duration>,
            limit: Option<usize>,
        ) -> ()
    );

    simple_request!(
        /// Evict a single key from a partial index. If no `eviction_request` is provided, a random
        /// key and partial index will be selected. Returns a [`SingleKeyEviction`] if an eviction
        /// occurred.
        evict_single(eviction_request: Option<SingleKeyEviction>) -> Option<SingleKeyEviction>
    );

    #[cfg(feature = "failure_injection")]
    simple_request!(
        /// Set a failpoint with provided name and action
        failpoint(name: String, action: String,) -> ()
    );

    simple_request!(
        /// Notify the controller that a running domain replica has died
        domain_died(replica_address: ReplicaAddress) -> ()
    );
}
