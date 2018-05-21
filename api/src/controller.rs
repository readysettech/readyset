// TODO: use dataflow::statistics::GraphStats;

use assert_infrequent;
use basics::*;
use consensus::{self, Authority};
use futures::Stream;
use getter::{GetterRpc, RemoteGetter, RemoteGetterBuilder};
use hyper::{self, Client};
use mutator::{Mutator, MutatorBuilder, MutatorRpc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio_core::reactor::Core;
use {ActivationResult, RpcError};

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
/// `RemoteGetter` and `Mutator` handles that are spawned from one `ControllerHandle` may share
/// underlying connections to Soup. This means that a `ControllerHandle` is *not* `Send` or `Sync`.
/// To establish more connections to Soup for use by other threads, use
/// `ControllerHandle::connect()` or call the `into_exclusive` method on a given getter or mutator.
pub struct ControllerHandle<A: Authority> {
    url: Option<String>,
    local_port: Option<u16>,
    authority: Arc<A>,
    getters: HashMap<(SocketAddr, usize), GetterRpc>,
    domains: HashMap<Vec<SocketAddr>, MutatorRpc>,
    reactor: Core,
    client: Client<hyper::client::HttpConnector>,
}

/// A pointer that lets you construct a new `ControllerHandle` from an existing one.
#[derive(Clone)]
pub struct ControllerPointer<A: Authority>(Arc<A>);

impl<A: Authority> ControllerPointer<A> {
    /// Construct another `ControllerHandle` to this controller.
    pub fn connect(&self) -> ControllerHandle<A> {
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
    pub fn make(authority: Arc<A>) -> Self {
        let core = Core::new().unwrap();
        let client = Client::configure()
            .connector(hyper::client::HttpConnector::new_with_executor(
                core.handle(),
                &core.handle(),
            ))
            .build(&core.handle());
        ControllerHandle {
            url: None,
            local_port: None,
            authority: authority,
            getters: Default::default(),
            domains: Default::default(),
            reactor: core,
            client: client,
        }
    }

    /// Create a `ControllerHandle` that bootstraps a connection to Soup via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `ControllerHandle::from_zk` instead.
    pub fn new(authority: A) -> Self {
        Self::make(Arc::new(authority))
    }

    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub fn from_zk(zookeeper_address: &str) -> ControllerHandle<consensus::ZookeeperAuthority> {
        ControllerHandle::new(consensus::ZookeeperAuthority::new(zookeeper_address))
    }

    #[doc(hidden)]
    pub fn rpc<Q: Serialize, R: DeserializeOwned>(&mut self, path: &str, request: Q) -> R {
        loop {
            if self.url.is_none() {
                let descriptor: ControllerDescriptor =
                    serde_json::from_slice(&self.authority.get_leader().unwrap().1).unwrap();
                self.url = Some(format!("http://{}", descriptor.external_addr));
            }
            let url = format!("{}/{}", self.url.as_ref().unwrap(), path);

            let mut r = hyper::Request::new(hyper::Method::Post, url.parse().unwrap());
            r.set_body(serde_json::to_vec(&request).unwrap());
            let res = self.reactor.run(self.client.request(r)).unwrap();
            if res.status() == hyper::StatusCode::ServiceUnavailable {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            if res.status() != hyper::StatusCode::Ok {
                self.url = None;
                continue;
            }

            let body = self.reactor.run(res.body().concat2()).unwrap();
            return serde_json::from_slice(&body).unwrap();
        }
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    pub fn inputs(&mut self) -> BTreeMap<String, NodeIndex> {
        self.rpc("inputs", &())
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE EXT VIEW` statement in a recipe.
    pub fn outputs(&mut self) -> BTreeMap<String, NodeIndex> {
        self.rpc("outputs", &())
    }

    fn get_getter_builder(&mut self, name: &str) -> Option<RemoteGetterBuilder> {
        // This call attempts to detect if `get_getter_builder` is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        self.rpc("getter_builder", &name)
    }

    /// Obtain a `RemoteGetter` that allows you to query the given external view.
    pub fn view(&mut self, name: &str) -> Option<RemoteGetter> {
        self.get_getter_builder(name).map(|mut g| {
            if let Some(port) = self.local_port {
                g = g.with_local_port(port);
            }

            let g = g.build(&mut self.getters);

            if self.local_port.is_none() {
                self.local_port = Some(g.local_addr().unwrap().port());
            }

            g
        })
    }

    fn get_mutator_builder(&mut self, base: &str) -> Option<MutatorBuilder> {
        // This call attempts to detect if `get_mutator_builder` is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        self.rpc("mutator_builder", &base)
    }

    /// Obtain a `Mutator` that allows you to perform writes, deletes, and other operations on the
    /// given base table.
    pub fn base(&mut self, name: &str) -> Option<Mutator> {
        self.get_mutator_builder(name).map(|mut m| {
            if let Some(port) = self.local_port {
                m = m.with_local_port(port);
            }

            let m = m.build(&mut self.domains);

            if self.local_port.is_none() {
                self.local_port = Some(m.local_addr().unwrap().port());
            }

            m
        })
    }

    ///// Get statistics about the time spent processing different parts of the graph.
    //pub fn get_statistics(&mut self) -> GraphStats {
    //    self.rpc("get_statistics", &())
    //}

    /// Flush all partial state, evicting all rows present.
    pub fn flush_partial(&mut self) -> Result<(), RpcError> {
        self.rpc("flush_partial", &())
    }

    /// Extend the existing recipe with the given set of queries.
    pub fn extend_recipe(&mut self, recipe_addition: &str) -> Result<ActivationResult, RpcError> {
        self.rpc("extend_recipe", recipe_addition)
    }

    /// Replace the existing recipe with this one.
    pub fn install_recipe(&mut self, new_recipe: &str) -> Result<ActivationResult, RpcError> {
        self.rpc("install_recipe", new_recipe)
    }

    /// Fetch a graphviz description of the dataflow graph.
    pub fn graphviz(&mut self) -> String {
        self.rpc("graphviz", &())
    }

    /// Remove the given external view from the graph.
    pub fn remove_node(&mut self, view: NodeIndex) {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", &view)
    }

    /// Close all connections opened by this handle.
    ///
    /// Any connections that are in use by `Mutator` or `RemoteGetter` handles that have not yet
    /// been dropped will be dropped only when those handles are dropped.
    ///
    /// Note that this method gets called automatically when the `ControllerHandle` is dropped.
    pub fn shutdown(&mut self) {
        self.getters.clear();
        self.domains.clear();
    }
}

impl<A: Authority> Drop for ControllerHandle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
