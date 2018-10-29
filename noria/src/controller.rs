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
use serde::Serialize;
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::prelude::*;
use tokio_tower::{buffer::Buffer, multiplex::Client};
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
    url: Option<String>,
    local_port: Option<u16>,
    authority: Arc<A>,
    domains: HashMap<Vec<SocketAddr>, TableRpc>,
    client: hyper::Client<hyper::client::HttpConnector>,
}

struct ControllerRequest {
    path: &'static str,
    request: hyper::Body,
}

impl ControllerRequest {
    fn new<Q: Serialize>(path: &'static str, r: Q) -> Result<Self, serde_json::Error> {
        Ok(ControllerRequest {
            path,
            request: serde_json::to_vec(&r)?.into(),
        })
    }
}

impl<A> Service for Controller<A>
where
    A: Authority,
{
    type Request = ControllerRequest;
    type Response = hyper::Chunk;
    type Error = failure::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let client = self.client.clone();
        let auth = self.authority.clone();
        let path = req.path;
        let r = hyper::Request::post(path).body(req.request).unwrap();

        Box::new(future::loop_fn((r, self.url.clone()), move |(r, url)| {
            let mut url = if let Some(url) = url {
                url
            } else {
                // TODO: don't do blocking things here...
                let descriptor: ControllerDescriptor =
                    serde_json::from_slice(&auth.get_leader()?.1)?;
                format!("http://{}/{}", descriptor.external_addr, path)
            };
            r.set_url(url);

            client
                .request(r)
                .and_then(|res| {
                    let status = res.status();
                    res.into_body().concat2().map(move |body| (status, body))
                })
                .and_then(move |(status, body)| match status {
                    hyper::StatusCode::OK => {
                        future::Either::B(future::Either::A(future::ok(future::Loop::Break(body))))
                    }
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
                            tokio::timer::Delay::new(Duration::from_millis(100))
                                .map(move || future::Loop::Continue(url)),
                        )
                    }
                })
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
    A: Authority,
{
    handle: Buffer<Controller<A>, ControllerRequest>,
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
            handle: Buffer::new(Controller {
                url: None,
                local_port: None,
                authority,
                views: Default::default(),
                domains: Default::default(),
                client: hyper::Client::new(),
            }),
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
            .and_then(|body| {
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
            .and_then(|body| {
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
            .call(ControllerRequest::new("view_builder", name).unwrap())
            .map_err(|e| format_err!("failed to fetch view builder: {:?}", e))
            .and_then(move |body| {
                match serde_json::from_slice::<Option<ViewBuilder>>(&body) {
                    Ok(Some(vb)) => {
                        // TODO
                        //if let Some(port) = self.local_port {
                        //    vb = vb.with_local_port(port);
                        //}

                        future::Either::A(vb.build(views).map_err(failure::Error::from))

                        //TODO
                        //if self.local_port.is_none() {
                        //    self.local_port = Some(g.local_addr().unwrap().port());
                        //}
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
    pub fn table(&mut self, name: &str) -> Result<Table, failure::Error> {
        // This call attempts to detect if this function is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        self.rpc::<_, Option<TableBuilder>>("table_builder", name)
            .context(format!("building Table for {}", name))?
            .ok_or_else(|| format_err!("view {} does not exist", name))
            .and_then(|mut m| {
                if let Some(port) = self.local_port {
                    m = m.with_local_port(port);
                }

                let m = m.build(&mut self.domains)?;

                if self.local_port.is_none() {
                    self.local_port = Some(m.local_addr().unwrap().port());
                }

                Ok(m)
            })
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn statistics(&mut self) -> Result<stats::GraphStats, failure::Error> {
        Ok(self.rpc("get_statistics", &()).context("getting stats")?)
    }

    /// Flush all partial state, evicting all rows present.
    pub fn flush_partial(&mut self) -> Result<(), failure::Error> {
        self.rpc("flush_partial", &())
            .context("flushing partial state")?;
        Ok(())
    }

    /// Extend the existing recipe with the given set of queries.
    pub fn extend_recipe(
        &mut self,
        recipe_addition: &str,
    ) -> Result<ActivationResult, failure::Error> {
        Ok(self
            .rpc::<_, ActivationResult>("extend_recipe", recipe_addition)
            .context(String::from(recipe_addition))?)
    }

    /// Replace the existing recipe with this one.
    pub fn install_recipe(&mut self, new_recipe: &str) -> Result<ActivationResult, failure::Error> {
        Ok(self
            .rpc::<_, ActivationResult>("install_recipe", new_recipe)
            .context(String::from(new_recipe))?)
    }

    /// Fetch a graphviz description of the dataflow graph.
    pub fn graphviz(&mut self) -> Result<String, failure::Error> {
        Ok(self
            .rpc("graphviz", &())
            .context("fetching graphviz representation")?)
    }

    /// Fetch a simplified graphviz description of the dataflow graph.
    pub fn simple_graphviz(&mut self) -> Result<String, failure::Error> {
        Ok(self
            .rpc("simple_graphviz", &())
            .context("fetching simple graphviz representation")?)
    }

    /// Remove the given external view from the graph.
    pub fn remove_node(&mut self, view: NodeIndex) -> Result<(), failure::Error> {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", &view)
            .context(format!("attempting to remove node {:?}", view))?;
        Ok(())
    }
}

impl<A> Drop for ControllerHandle<A>
where
    A: Authority,
{
    fn drop(&mut self) {
        drop(self.req.take());
        self.rt.take().unwrap().join().unwrap();
    }
}
