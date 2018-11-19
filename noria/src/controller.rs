#[cfg(debug_assertions)]
use assert_infrequent;
use crate::consensus::{self, Authority};
use crate::debug::stats;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::ActivationResult;
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

struct ControllerRequest {
    path: &'static str,
    request: Vec<u8>,
}

impl ControllerRequest {
    fn new<Q: Serialize>(path: &'static str, r: Q) -> Result<Self, serde_json::Error> {
        Ok(ControllerRequest {
            path,
            request: serde_json::to_vec(&r)?.into(),
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
                    .and_then(|res| {
                        let status = res.status();
                        res.into_body().concat2().map(move |body| (status, body))
                    })
                    .map_err(|he| {
                        failure::Error::from(he)
                            .context("hyper request failed")
                            .into()
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
#[derive(Clone)]
pub struct ControllerHandle<A>
where
    A: 'static + Authority,
{
    handle: Buffer<Controller<A>, ControllerRequest>,
    domains: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    views: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
}

impl ControllerHandle<consensus::ZookeeperAuthority> {
    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub fn from_zk(zookeeper_address: &str) -> Result<Self, failure::Error> {
        Ok(ControllerHandle::new(consensus::ZookeeperAuthority::new(
            zookeeper_address,
        )?))
    }
}

impl<A: Authority> ControllerHandle<A> {
    #[doc(hidden)]
    pub fn make(authority: Arc<A>) -> Self {
        ControllerHandle {
            views: Default::default(),
            domains: Default::default(),
            handle: Buffer::new(
                Controller {
                    authority,
                    client: hyper::Client::new(),
                },
                0,
                &tokio::executor::DefaultExecutor::current(),
            )
            .unwrap_or_else(|_| panic!("no running tokio executor")),
        }
    }

    /// Create a `ControllerHandle` that bootstraps a connection to Noria via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `ControllerHandle::from_zk` instead.
    pub fn new(authority: A) -> Self {
        Self::make(Arc::new(authority))
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    pub fn inputs(
        &mut self,
    ) -> impl Future<Item = BTreeMap<String, NodeIndex>, Error = failure::Error> {
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
    ) -> impl Future<Item = BTreeMap<String, NodeIndex>, Error = failure::Error> {
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
    pub fn view(mut self, name: &str) -> impl Future<Item = View, Error = failure::Error> {
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
    pub fn table(&mut self, name: &str) -> impl Future<Item = Table, Error = failure::Error> {
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

    // TODO:
    // this method currently assumes that the returned future is tied to the lifetime of Q.
    // the fix for this is to use existential types, because there's no way to make impl Trait
    // *not* associate itself with all provided type arguments :'(
    // once we're using existentials, we can remove String::from from callers
    fn rpc<Q: Serialize, R>(
        &mut self,
        path: &'static str,
        r: Q,
        err: &'static str,
    ) -> impl Future<Item = R, Error = failure::Error>
    where
        for<'de> R: Deserialize<'de>,
    {
        self.handle
            .call(ControllerRequest::new(path, r).unwrap())
            .map_err(move |e| format_err!("{}: {:?}", err, e))
            .and_then(move |body: hyper::Chunk| {
                serde_json::from_slice::<R>(&body)
                    .context("failed to response")
                    .context(err)
                    .map_err(failure::Error::from)
            })
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn statistics(&mut self) -> impl Future<Item = stats::GraphStats, Error = failure::Error> {
        self.rpc("get_statistics", &(), "failed to get stats")
    }

    /// Flush all partial state, evicting all rows present.
    pub fn flush_partial(&mut self) -> impl Future<Item = (), Error = failure::Error> {
        self.rpc("flush_partial", &(), "failed to flush partial")
    }

    /// Extend the existing recipe with the given set of queries.
    pub fn extend_recipe(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Item = ActivationResult, Error = failure::Error> {
        self.rpc(
            "extend_recipe",
            String::from(recipe_addition),
            "failed to extend recipe",
        )
    }

    /// Replace the existing recipe with this one.
    pub fn install_recipe(
        &mut self,
        new_recipe: &str,
    ) -> impl Future<Item = ActivationResult, Error = failure::Error> {
        self.rpc(
            "install_recipe",
            String::from(new_recipe),
            "failed to install recipe",
        )
    }

    /// Fetch a graphviz description of the dataflow graph.
    pub fn graphviz(&mut self) -> impl Future<Item = String, Error = failure::Error> {
        self.rpc("graphviz", &(), "failed to fetch graphviz output")
    }

    /// Fetch a simplified graphviz description of the dataflow graph.
    pub fn simple_graphviz(&mut self) -> impl Future<Item = String, Error = failure::Error> {
        self.rpc(
            "simple_graphviz",
            &(),
            "failed to fetch simple graphviz output",
        )
    }

    /// Remove the given external view from the graph.
    pub fn remove_node(
        &mut self,
        view: NodeIndex,
    ) -> impl Future<Item = (), Error = failure::Error> {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", view, "failed to remove node")
    }
}
