use assert_infrequent;
use basics::*;
use consensus::{self, Authority};
use debug::stats;
use failure::{self, ResultExt};
use futures::Stream;
use hyper::{self, Client};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use table::{Table, TableBuilder, TableRpc};
use tokio;
use view::{View, ViewBuilder, ViewRpc};
use ActivationResult;

/// Describes a running controller instance.
///
/// A serialized version of this struct is stored in ZooKeeper so that clients can reach the
/// currently active controller.
#[derive(Clone, Serialize, Deserialize)]
#[doc(hidden)]
pub struct ControllerDescriptor {
    pub external_addr: SocketAddr,
    pub internal_addr: SocketAddr,
    pub nonce: u64,
}

/// A handle to a Soup controller.
///
/// This handle is the primary mechanism for interacting with a running Soup instance, and lets you
/// add and remove queries, retrieve handles for inserting or querying the underlying data, and to
/// perform meta-operations such as fetching the dataflow's GraphViz visualization.
///
/// To establish a new connection to Soup, use `ControllerHandle::new`, and pass in the appropraite
/// `Authority`. In the likely case that you are using Zookeeper, use `ControllerHandle::from_zk`.
///
/// `View` and `Table` handles that are spawned from one `ControllerHandle` may share
/// underlying connections to Soup. This means that a `ControllerHandle` is *not* `Send` or `Sync`.
/// To establish more connections to Soup for use by other threads, use
/// `ControllerHandle::connect()` or call the `into_exclusive` method on a given view or table.
pub struct ControllerHandle<A: Authority> {
    url: Option<String>,
    local_port: Option<u16>,
    authority: Arc<A>,
    views: HashMap<(SocketAddr, usize), ViewRpc>,
    domains: HashMap<Vec<SocketAddr>, TableRpc>,
    client: Client<hyper::client::HttpConnector>,
    rt: tokio::runtime::current_thread::Runtime,
}

/// A pointer that lets you construct a new `ControllerHandle` from an existing one.
#[derive(Clone)]
pub struct ControllerPointer<A: Authority>(Arc<A>);

impl<A: Authority> ControllerPointer<A> {
    /// Construct another `ControllerHandle` to this controller.
    pub fn connect(&self) -> Result<ControllerHandle<A>, failure::Error> {
        ControllerHandle::make(self.0.clone())
    }
}

impl<A: Authority> ControllerHandle<A> {
    /// Create a pointer to the controller pointed to by this handle.
    ///
    /// This method is safe to call from other threads than the one that made the
    /// `ControllerHandle`. Note however that if `Self = LocalControllerHandle` then dropping the
    /// `LocalControllerHandle` will still cause the controller threads to shut down, and thus any
    /// other `ControllerHandle` instances will stop working.
    pub fn pointer(&self) -> ControllerPointer<A> {
        ControllerPointer(self.authority.clone())
    }

    /// Return the URL endpoint of the currently active controller.
    ///
    /// Note that this may change over time in response to failures.
    pub fn url(&self) -> Option<&str> {
        self.url.as_ref().map(|s| &**s)
    }

    #[doc(hidden)]
    pub fn make(authority: Arc<A>) -> Result<Self, failure::Error> {
        Ok(ControllerHandle {
            url: None,
            local_port: None,
            authority,
            client: Client::new(),
            views: Default::default(),
            domains: Default::default(),
            rt: tokio::runtime::current_thread::Runtime::new()?,
        })
    }

    /// Create a `ControllerHandle` that bootstraps a connection to Soup via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `ControllerHandle::from_zk` instead.
    pub fn new(authority: A) -> Result<Self, failure::Error> {
        Self::make(Arc::new(authority))
    }

    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub fn from_zk(
        zookeeper_address: &str,
    ) -> Result<ControllerHandle<consensus::ZookeeperAuthority>, failure::Error> {
        ControllerHandle::new(consensus::ZookeeperAuthority::new(zookeeper_address)?)
    }

    #[doc(hidden)]
    pub fn rpc<Q: Serialize, R: DeserializeOwned>(
        &mut self,
        path: &str,
        request: Q,
    ) -> Result<R, failure::Error> {
        loop {
            if self.url.is_none() {
                let descriptor: ControllerDescriptor =
                    serde_json::from_slice(&self.authority.get_leader().unwrap().1).unwrap();
                self.url = Some(format!("http://{}", descriptor.external_addr));
            }
            let url = format!("{}/{}", self.url.as_ref().unwrap(), path);

            let r = hyper::Request::post(url)
                .body(serde_json::to_vec(&request)?.into())
                .unwrap();
            let res = self.rt.block_on(self.client.request(r))?;
            match res.status() {
                hyper::StatusCode::SERVICE_UNAVAILABLE => {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                status @ hyper::StatusCode::INTERNAL_SERVER_ERROR
                | status @ hyper::StatusCode::OK => {
                    let body = self.rt.block_on(res.into_body().concat2())?;
                    if let hyper::StatusCode::OK = status {
                        return Ok(serde_json::from_slice::<R>(&body)
                            .context(format!("while decoding rpc reply from {}", path))?);
                    } else {
                        bail!(
                            serde_json::from_slice::<String>(&body)
                                .context(format!("while decoding rpc error reply from {}", path))?
                        );
                    }
                }
                _ => {
                    self.url = None;
                    continue;
                }
            }
        }
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    pub fn inputs(&mut self) -> Result<BTreeMap<String, NodeIndex>, failure::Error> {
        self.rpc("inputs", &())
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE EXT VIEW` statement in a recipe.
    pub fn outputs(&mut self) -> Result<BTreeMap<String, NodeIndex>, failure::Error> {
        self.rpc("outputs", &())
    }

    /// Obtain a `View` that allows you to query the given external view.
    pub fn view(&mut self, name: &str) -> Result<View, failure::Error> {
        // This call attempts to detect if this function is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        self.rpc::<_, Option<ViewBuilder>>("view_builder", name)
            .context(format!("building View for {}", name))?
            .ok_or_else(|| format_err!("view {} does not exist", name))
            .and_then(|mut g| {
                if let Some(port) = self.local_port {
                    g = g.with_local_port(port);
                }

                let g = g.build(&mut self.views)?;

                if self.local_port.is_none() {
                    self.local_port = Some(g.local_addr().unwrap().port());
                }

                Ok(g)
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
            .rpc("extend_recipe", recipe_addition)
            .context(format!("extending recipe with : {}", recipe_addition))?)
    }

    /// Replace the existing recipe with this one.
    pub fn install_recipe(&mut self, new_recipe: &str) -> Result<ActivationResult, failure::Error> {
        Ok(self
            .rpc("install_recipe", new_recipe)
            .context(format!("installing new recipe: {}", new_recipe))?)
    }

    /// Fetch a graphviz description of the dataflow graph.
    pub fn graphviz(&mut self) -> Result<String, failure::Error> {
        Ok(self
            .rpc("graphviz", &())
            .context("fetching graphviz representation")?)
    }

    /// Remove the given external view from the graph.
    pub fn remove_node(&mut self, view: NodeIndex) -> Result<(), failure::Error> {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", &view)
            .context(format!("attempting to remove node {:?}", view))?;
        Ok(())
    }

    /// Close all connections opened by this handle.
    ///
    /// Any connections that are in use by `Table` or `View` handles that have not yet
    /// been dropped will be dropped only when those handles are dropped.
    ///
    /// Note that this method gets called automatically when the `ControllerHandle` is dropped.
    pub fn shutdown(&mut self) {
        self.views.clear();
        self.domains.clear();
    }
}

impl<A: Authority> Drop for ControllerHandle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
