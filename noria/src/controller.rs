#[cfg(debug_assertions)]
use assert_infrequent;
use crate::consensus::{self, Authority};
use crate::debug::stats;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::ActivationResult;
use failure::{self, ResultExt};
use futures::{
    sync::{mpsc, oneshot},
    Future, Stream,
};
use hyper::{self, Client};
use petgraph::graph::NodeIndex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio;

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
pub struct ControllerHandle<A> {
    url: Option<String>,
    local_port: Option<u16>,
    authority: Arc<A>,
    views: HashMap<(SocketAddr, usize), ViewRpc>,
    domains: HashMap<Vec<SocketAddr>, TableRpc>,
    req: Option<
        mpsc::UnboundedSender<(
            hyper::Request<hyper::Body>,
            oneshot::Sender<(hyper::StatusCode, hyper::Chunk)>,
        )>,
    >,
    rt: Option<thread::JoinHandle<()>>,
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

impl ControllerHandle<consensus::ZookeeperAuthority> {
    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub fn from_zk(zookeeper_address: &str) -> Result<Self, failure::Error> {
        ControllerHandle::new(consensus::ZookeeperAuthority::new(zookeeper_address)?)
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
        let (tx, rx) = mpsc::unbounded();
        let rt = thread::Builder::new()
            .name("api".to_string())
            .spawn(move || {
                let client = Client::new();
                let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
                rt.spawn(
                    rx.map_err(|_| unreachable!())
                        .for_each(
                            move |(req, tx): (
                                hyper::Request<hyper::Body>,
                                oneshot::Sender<(hyper::StatusCode, hyper::Chunk)>,
                            )| {
                                client
                                    .request(req)
                                    .and_then(|res| {
                                        let status = res.status();
                                        res.into_body().concat2().map(move |body| (status, body))
                                    })
                                    .and_then(move |r| tx.send(r).map_err(|_| unreachable!()))
                            },
                        )
                        .map_err(|_| unreachable!()),
                );
                rt.run().unwrap()
            })?;

        Ok(ControllerHandle {
            url: None,
            local_port: None,
            authority,
            views: Default::default(),
            domains: Default::default(),
            req: Some(tx),
            rt: Some(rt),
        })
    }

    /// Create a `ControllerHandle` that bootstraps a connection to Soup via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `ControllerHandle::from_zk` instead.
    pub fn new(authority: A) -> Result<Self, failure::Error> {
        Self::make(Arc::new(authority))
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
                    serde_json::from_slice(&self.authority.get_leader()?.1)?;
                self.url = Some(format!("http://{}", descriptor.external_addr));
            }
            let url = format!("{}/{}", self.url.as_ref().unwrap(), path);

            let r = hyper::Request::post(url)
                .body(serde_json::to_vec(&request)?.into())
                .unwrap();
            let (tx, rx) = oneshot::channel();
            self.req.as_mut().unwrap().unbounded_send((r, tx)).unwrap();
            let (status, body) = rx.wait()?;
            match status {
                hyper::StatusCode::SERVICE_UNAVAILABLE => {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                hyper::StatusCode::OK => {
                    return Ok(serde_json::from_slice::<R>(&body).context(format!(
                        "while decoding rpc reply for {}: {}",
                        path,
                        String::from_utf8_lossy(&*body)
                    ))?);
                }
                hyper::StatusCode::INTERNAL_SERVER_ERROR => {
                    bail!(
                        "rpc call to {} failed: {}",
                        path,
                        String::from_utf8_lossy(&*body)
                    );
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
        // This call attempts to detect if this function is being called in a loop. If this is
        // getting false positives, then it is safe to increase the allowed hit count, however, the
        // limit_mutator_creation test in src/controller/handle.rs should then be updated as well.
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

impl<A> Drop for ControllerHandle<A> {
    fn drop(&mut self) {
        drop(self.req.take());
        self.rt.take().unwrap().join().unwrap();
    }
}
