use crate::consensus::{self, Authority};
use crate::debug::stats;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::ActivationResult;
#[cfg(debug_assertions)]
use assert_infrequent;
use failure::{self, ResultExt};
use hyper;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tower_buffer::Buffer;
use tower_service::Service;

/// Describes a running controller instance.
///
/// A serialized version of this struct is stored in ZooKeeper so that clients can reach the
/// currently active controller.
#[derive(Clone, Serialize, Deserialize)]
#[doc(hidden)]
pub struct ControllerDescriptor {
    pub external_addr: SocketAddr,
    pub worker_addr: SocketAddr,
    pub domain_addr: SocketAddr,
    pub nonce: u64,
}

struct Controller<A> {
    authority: Arc<A>,
    client: hyper::Client<hyper::client::HttpConnector>,
}

#[derive(Debug)]
struct ControllerRequest {
    path: &'static str,
    request: Vec<u8>,
}

impl ControllerRequest {
    fn new<Q: Serialize>(path: &'static str, r: Q) -> Result<Self, serde_json::Error> {
        Ok(ControllerRequest {
            path,
            request: serde_json::to_vec(&r)?,
        })
    }
}

impl<A> Service<ControllerRequest> for Controller<A>
where
    A: 'static + Authority,
{
    type Response = hyper::Chunk;
    type Error = failure::Error;

    // TODO: existential type
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: ControllerRequest) -> Self::Future {
        let client = self.client.clone();
        let auth = self.authority.clone();
        let path = req.path;
        let body = req.request;

        Box::new(future::loop_fn(None, move |url| {
            let url = if let Some(url) = url {
                url
            } else {
                // TODO: don't do blocking things here...
                // TODO: cache this value?
                let descriptor: Result<ControllerDescriptor, _> = try {
                    serde_json::from_slice(
                        &auth.get_leader().context("failed to get current leader")?.1,
                    )
                    .context("failed to deserialize authority reply")?
                };

                let descriptor = match descriptor {
                    Ok(d) => d,
                    Err(e) => return future::Either::A(future::err(e)),
                };
                format!("http://{}/{}", descriptor.external_addr, path)
            };

            let r = hyper::Request::post(&url)
                .body(hyper::Body::from(body.clone()))
                .unwrap();

            future::Either::B(
                client
                    .request(r)
                    .map_err(|he| {
                        failure::Error::from(he)
                            .context("hyper request failed")
                            .into()
                    })
                    .and_then(|res| {
                        let status = res.status();
                        res.into_body()
                            .concat2()
                            .map(move |body| (status, body))
                            .map_err(|he| {
                                failure::Error::from(he)
                                    .context("hyper response failed")
                                    .into()
                            })
                    })
                    .and_then(move |(status, body)| match status {
                        hyper::StatusCode::OK => future::Either::B(future::Either::A(future::ok(
                            future::Loop::Break(body),
                        ))),
                        hyper::StatusCode::INTERNAL_SERVER_ERROR => {
                            future::Either::B(future::Either::B(future::err(format_err!(
                                "rpc call to {} failed: {}",
                                path,
                                String::from_utf8_lossy(&*body)
                            ))))
                        }
                        s => {
                            let url = if s == hyper::StatusCode::SERVICE_UNAVAILABLE {
                                None
                            } else {
                                Some(url)
                            };

                            future::Either::A(
                                tokio::timer::Delay::new(
                                    Instant::now() + Duration::from_millis(100),
                                )
                                .map(move |_| future::Loop::Continue(url))
                                .map_err(|_| {
                                    failure::err_msg("tokio timer went away during http retry")
                                }),
                            )
                        }
                    }),
            )
        }))
    }
}

/// A handle to a Noria controller.
///
/// This handle is the primary mechanism for interacting with a running Noria instance, and lets
/// you add and remove queries, retrieve handles for inserting or querying the underlying data, and
/// to perform meta-operations such as fetching the dataflow's GraphViz visualization.
///
/// To establish a new connection to Noria, use `ControllerHandle::new`, and pass in the
/// appropriate `Authority`. In the likely case that you are using Zookeeper, use
/// `ControllerHandle::from_zk`.
///
/// Note that whatever Tokio Runtime you use to execute the `Future` that resolves into the
/// `ControllerHandle` will also be the one that executes all your reads and writes through `View`
/// and `Table`. Make sure that that `Runtime` stays alive, and continues to be driven, otherwise
/// none of your operations will ever complete! Furthermore, you *must* use the `Runtime` to
/// execute any futures returned from `ControllerHandle` (that is, you cannot just call `.wait()`
/// on them).
// TODO: this should be renamed to NoriaHandle, or maybe just Connection, since it also provides
// reads and writes, which aren't controller actions!
pub struct ControllerHandle<A>
where
    A: 'static + Authority,
{
    handle: Buffer<Controller<A>, ControllerRequest>,
    domains: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    views: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
}

impl<A> Clone for ControllerHandle<A>
where
    A: 'static + Authority,
{
    fn clone(&self) -> Self {
        ControllerHandle {
            handle: self.handle.clone(),
            domains: self.domains.clone(),
            views: self.views.clone(),
        }
    }
}

impl ControllerHandle<consensus::ZookeeperAuthority> {
    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub fn from_zk(zookeeper_address: &str) -> impl Future<Item = Self, Error = failure::Error> {
        match consensus::ZookeeperAuthority::new(zookeeper_address) {
            Ok(auth) => future::Either::A(ControllerHandle::new(auth)),
            Err(e) => future::Either::B(future::err(e)),
        }
    }
}

impl<A: Authority + 'static> ControllerHandle<A> {
    #[doc(hidden)]
    pub fn make(authority: Arc<A>) -> impl Future<Item = Self, Error = failure::Error> {
        // need to use lazy otherwise current executor won't be known
        future::lazy(move || {
            Ok(ControllerHandle {
                views: Default::default(),
                domains: Default::default(),
                handle: Buffer::new(
                    Controller {
                        authority,
                        client: hyper::Client::new(),
                    },
                    1,
                )
                .unwrap_or_else(|_| panic!("no running tokio executor")),
            })
        })
    }

    /// Create a `ControllerHandle` that bootstraps a connection to Noria via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `ControllerHandle::from_zk` instead.
    pub fn new(authority: A) -> impl Future<Item = Self, Error = failure::Error> + Send
    where
        A: Send + 'static,
    {
        Self::make(Arc::new(authority))
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    pub fn inputs(
        &mut self,
    ) -> impl Future<Item = BTreeMap<String, NodeIndex>, Error = failure::Error> + Send {
        self.handle
            .call(ControllerRequest::new("inputs", &()).unwrap())
            .map_err(|e| format_err!("failed to fetch inputs: {:?}", e))
            .and_then(|body: hyper::Chunk| {
                serde_json::from_slice(&body)
                    .context("couldn't parse input response")
                    .map_err(failure::Error::from)
            })
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE EXT VIEW` statement in a recipe.
    pub fn outputs(
        &mut self,
    ) -> impl Future<Item = BTreeMap<String, NodeIndex>, Error = failure::Error> + Send {
        self.handle
            .call(ControllerRequest::new("outputs", &()).unwrap())
            .map_err(|e| format_err!("failed to fetch outputs: {:?}", e))
            .and_then(|body: hyper::Chunk| {
                serde_json::from_slice(&body)
                    .context("couldn't parse output response")
                    .map_err(failure::Error::from)
            })
    }

    /// Obtain a `View` that allows you to query the given external view.
    pub fn view(&mut self, name: &str) -> impl Future<Item = View, Error = failure::Error> + Send {
        // This call attempts to detect if this function is being called in a loop. If this is
        // getting false positives, then it is safe to increase the allowed hit count, however, the
        // limit_mutator_creation test in src/controller/handle.rs should then be updated as well.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        let views = self.views.clone();
        let name = name.to_string();
        self.handle
            .call(ControllerRequest::new("view_builder", &name).unwrap())
            .map_err(|e| format_err!("failed to fetch view builder: {:?}", e))
            .and_then(move |body: hyper::Chunk| {
                match serde_json::from_slice::<Option<ViewBuilder>>(&body) {
                    Ok(Some(vb)) => {
                        future::Either::A(vb.build(views).map_err(failure::Error::from))
                    }
                    Ok(None) => {
                        future::Either::B(future::err(failure::err_msg("view does not exist")))
                    }
                    Err(e) => future::Either::B(future::err(failure::Error::from(e))),
                }
                .map_err(move |e| e.context(format!("building view for {}", name)).into())
            })
    }

    /// Obtain a `Table` that allows you to perform writes, deletes, and other operations on the
    /// given base table.
    pub fn table(
        &mut self,
        name: &str,
    ) -> impl Future<Item = Table, Error = failure::Error> + Send {
        // This call attempts to detect if this function is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        let domains = self.domains.clone();
        let name = name.to_string();
        self.handle
            .call(ControllerRequest::new("table_builder", &name).unwrap())
            .map_err(|e| format_err!("failed to fetch table builder: {:?}", e))
            .and_then(move |body: hyper::Chunk| {
                match serde_json::from_slice::<Option<TableBuilder>>(&body) {
                    Ok(Some(tb)) => future::Either::A(
                        tb.build(domains)
                            .into_future()
                            .map_err(failure::Error::from),
                    ),
                    Ok(None) => {
                        future::Either::B(future::err(failure::err_msg("view table not exist")))
                    }
                    Err(e) => future::Either::B(future::err(failure::Error::from(e))),
                }
                .map_err(move |e| e.context(format!("building table for {}", name)).into())
            })
    }

    // TODO: we can't use impl Trait here, because it would assume that the returned future is tied
    // to the lifetime of Q, which is not the case. existential types fix this issue.
    #[doc(hidden)]
    pub fn rpc<Q: Serialize, R: 'static>(
        &mut self,
        path: &'static str,
        r: Q,
        err: &'static str,
    ) -> Box<Future<Item = R, Error = failure::Error> + Send>
    where
        for<'de> R: Deserialize<'de>,
        R: Send,
    {
        Box::new(
            self.handle
                .call(ControllerRequest::new(path, r).unwrap())
                .map_err(move |e| format_err!("{}: {:?}", err, e))
                .and_then(move |body: hyper::Chunk| {
                    serde_json::from_slice::<R>(&body)
                        .context("failed to response")
                        .context(err)
                        .map_err(failure::Error::from)
                }),
        )
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn statistics(
        &mut self,
    ) -> impl Future<Item = stats::GraphStats, Error = failure::Error> + Send {
        self.rpc("get_statistics", (), "failed to get stats")
    }

    /// Flush all partial state, evicting all rows present.
    pub fn flush_partial(&mut self) -> impl Future<Item = (), Error = failure::Error> + Send {
        self.rpc("flush_partial", (), "failed to flush partial")
    }

    /// Extend the existing recipe with the given set of queries.
    pub fn extend_recipe(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Item = ActivationResult, Error = failure::Error> + Send {
        self.rpc("extend_recipe", recipe_addition, "failed to extend recipe")
    }

    /// Replace the existing recipe with this one.
    pub fn install_recipe(
        &mut self,
        new_recipe: &str,
    ) -> impl Future<Item = ActivationResult, Error = failure::Error> + Send {
        self.rpc("install_recipe", new_recipe, "failed to install recipe")
    }

    /// Fetch a graphviz description of the dataflow graph.
    pub fn graphviz(&mut self) -> impl Future<Item = String, Error = failure::Error> + Send {
        self.rpc("graphviz", (), "failed to fetch graphviz output")
    }

    /// Fetch a simplified graphviz description of the dataflow graph.
    pub fn simple_graphviz(&mut self) -> impl Future<Item = String, Error = failure::Error> + Send {
        self.rpc(
            "simple_graphviz",
            (),
            "failed to fetch simple graphviz output",
        )
    }

    /// Remove the given external view from the graph.
    pub fn remove_node(
        &mut self,
        view: NodeIndex,
    ) -> impl Future<Item = (), Error = failure::Error> + Send {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", view, "failed to remove node")
    }

    /// Construct a synchronous interface to this controller instance using the given executor to
    /// execute all operations.
    ///
    /// Note that the given executor *must* be the same as the one used to construct this
    /// `ControllerHandle`.
    pub fn sync_handle<E>(&self, executor: E) -> SyncControllerHandle<A, E>
    where
        E: tokio::executor::Executor,
    {
        SyncControllerHandle {
            executor,
            handle: self.clone(),
        }
    }
}

/// A synchronous handle to a Noria controller.
///
/// This handle lets you interact with a Noria controller without thinking about asynchrony.
pub struct SyncControllerHandle<A, E>
where
    A: 'static + Authority,
{
    handle: ControllerHandle<A>,
    executor: E,
}

impl<A, E> Clone for SyncControllerHandle<A, E>
where
    A: Authority,
    E: Clone,
{
    fn clone(&self) -> Self {
        SyncControllerHandle {
            handle: self.handle.clone(),
            executor: self.executor.clone(),
        }
    }
}

impl<E> SyncControllerHandle<consensus::ZookeeperAuthority, E>
where
    E: tokio::executor::Executor,
{
    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub fn from_zk(zookeeper_address: &str, executor: E) -> Result<Self, failure::Error> {
        Self::new(
            consensus::ZookeeperAuthority::new(zookeeper_address)?,
            executor,
        )
    }
}

impl<A, E> SyncControllerHandle<A, E>
where
    A: Authority,
    E: tokio::executor::Executor,
{
    /// Create a `SyncControllerHandle` that bootstraps a connection to Noria via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `SyncControllerHandle::from_zk` instead.
    pub fn new(authority: A, mut executor: E) -> Result<Self, failure::Error>
    where
        A: Send + 'static,
    {
        let fut = ControllerHandle::new(authority);
        let (tx, rx) = futures::sync::oneshot::channel();
        executor
            .spawn(Box::new(
                fut.then(move |r| tx.send(r)).map_err(|_| unreachable!()),
            ))
            .expect("runtime went away");
        let handle = rx.wait().expect("runtime went away")?;
        Ok(SyncControllerHandle { executor, handle })
    }

    #[doc(hidden)]
    pub fn run<F>(&mut self, fut: F) -> Result<F::Item, F::Error>
    where
        F: IntoFuture,
        <F as IntoFuture>::Future: Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        let (tx, rx) = futures::sync::oneshot::channel();
        self.executor
            .spawn(Box::new(
                fut.into_future()
                    .then(move |r| tx.send(r))
                    .map_err(|_| unreachable!()),
            ))
            .expect("runtime went away");
        rx.wait().expect("runtime went away")
    }

    /// Get a handle to the underlying asynchronous controller handle.
    pub fn handle(&mut self) -> &mut ControllerHandle<A> {
        &mut self.handle
    }

    /// Enumerate all known base tables.
    ///
    /// See [`ControllerHandle::inputs`].
    pub fn inputs(&mut self) -> Result<BTreeMap<String, NodeIndex>, failure::Error> {
        let fut = self.handle.inputs();
        self.run(fut)
    }

    /// Enumerate all known external views.
    ///
    /// See [`ControllerHandle::outputs`].
    pub fn outputs(&mut self) -> Result<BTreeMap<String, NodeIndex>, failure::Error> {
        let fut = self.handle.outputs();
        self.run(fut)
    }

    /// Get a handle to a [`Table`].
    ///
    /// See [`ControllerHandle::table`].
    pub fn table<S: AsRef<str>>(&mut self, table: S) -> Result<Table, failure::Error> {
        let fut = self.handle.table(table.as_ref());
        self.run(fut)
    }

    /// Get a handle to a [`View`].
    ///
    /// See [`ControllerHandle::view`].
    pub fn view<S: AsRef<str>>(&mut self, view: S) -> Result<View, failure::Error> {
        let fut = self.handle.view(view.as_ref());
        self.run(fut)
    }

    /// Install a Noria recipe.
    ///
    /// See [`ControllerHandle::install_recipe`].
    pub fn install_recipe<S: AsRef<str>>(
        &mut self,
        r: S,
    ) -> Result<ActivationResult, failure::Error> {
        let fut = self.handle.install_recipe(r.as_ref());
        self.run(fut)
    }

    /// Extend the Noria recipe.
    ///
    /// See [`ControllerHandle::extend_recipe`].
    pub fn extend_recipe<S: AsRef<str>>(
        &mut self,
        r: S,
    ) -> Result<ActivationResult, failure::Error> {
        let fut = self.handle.extend_recipe(r.as_ref());
        self.run(fut)
    }

    /// Fetch a graphviz description of the dataflow graph.
    ///
    /// See [`ControllerHandle::graphviz`].
    pub fn graphviz(&mut self) -> Result<String, failure::Error> {
        let fut = self.handle.graphviz();
        self.run(fut)
    }

    /// Remove the given external view from the graph.
    ///
    /// See [`ControllerHandle::remove_node`].
    pub fn remove_node(&mut self, view: NodeIndex) -> Result<(), failure::Error> {
        let fut = self.handle.remove_node(view);
        self.run(fut)
    }
}
