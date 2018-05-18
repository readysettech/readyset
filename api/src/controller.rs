use basics::*;
//use dataflow::statistics::GraphStats;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use assert_infrequent;
use futures::Stream;
use hyper::{self, Client};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use tarpc::sync::client::{self, ClientExt};
use tokio_core::reactor::Core;

use consensus::Authority;
use getter::{GetterRpc, RemoteGetter, RemoteGetterBuilder};
use mutator::{Mutator, MutatorBuilder, MutatorRpc};
use {ActivationResult, RpcError};

/// Describes a running controller instance. A serialized version of this struct is stored in
/// ZooKeeper so that clients can reach the currently active controller.
#[derive(Clone, Serialize, Deserialize)]
pub struct ControllerDescriptor {
    pub external_addr: SocketAddr,
    pub internal_addr: SocketAddr,
    pub checktable_addr: SocketAddr,
    pub nonce: u64,
}

/// `ControllerHandle` is a handle to a Controller.
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
    /// Construct another `ControllerHandle` to the controller this recipe
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

    /// Creates a `ControllerHandle` that bootstraps a connection to Soup via the configuration
    /// stored in the `Authority` passed as an argument.
    pub fn new(authority: A) -> Self {
        Self::make(Arc::new(authority))
    }

    #[doc(hidden)]
    pub fn rpc<Q: Serialize, R: DeserializeOwned>(&mut self, path: &str, request: &Q) -> R {
        loop {
            if self.url.is_none() {
                let descriptor: ControllerDescriptor =
                    serde_json::from_slice(&self.authority.get_leader().unwrap().1).unwrap();
                self.url = Some(format!("http://{}", descriptor.external_addr));
            }
            let url = format!("{}/{}", self.url.as_ref().unwrap(), path);

            let mut r = hyper::Request::new(hyper::Method::Post, url.parse().unwrap());
            r.set_body(serde_json::to_vec(request).unwrap());
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

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Base`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    pub fn inputs(&mut self) -> BTreeMap<String, NodeIndex> {
        self.rpc("inputs", &())
    }

    /// Get a Vec of to all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    pub fn outputs(&mut self) -> BTreeMap<String, NodeIndex> {
        self.rpc("outputs", &())
    }

    /// Obtain a `RemoteGetterBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node.
    fn get_getter_builder(&mut self, name: &str) -> Option<RemoteGetterBuilder> {
        // This call attempts to detect if `get_getter_builder` is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        self.rpc("getter_builder", &name)
    }

    /// Obtain a `RemoteGetter`.
    pub fn get_getter(&mut self, name: &str) -> Option<RemoteGetter> {
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

    /// Obtain a MutatorBuild that can be used to construct a Mutator to perform writes and deletes
    /// from the given base node.
    fn get_mutator_builder(&mut self, base: &str) -> Option<MutatorBuilder> {
        // This call attempts to detect if `get_mutator_builder` is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        self.rpc("mutator_builder", &base)
    }

    /// Obtain a Mutator
    pub fn get_mutator(&mut self, base: &str) -> Option<Mutator> {
        self.get_mutator_builder(base).map(|mut m| {
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

    /// Get statistics about the time spent processing different parts of the graph.
    //pub fn get_statistics(&mut self) -> GraphStats {
    //    self.rpc("get_statistics", &())
    //}

    /// Flush all partial state, evicting all rows present.
    pub fn flush_partial(&mut self) -> Result<(), RpcError> {
        self.rpc("flush_partial", &())
    }

    /// Extend the existing recipe on the controller by adding a new query.
    pub fn extend_recipe(&mut self, recipe_addition: String) -> Result<ActivationResult, RpcError> {
        self.rpc("extend_recipe", &recipe_addition)
    }

    /// Install a new recipe on the controller.
    pub fn install_recipe(&mut self, new_recipe: String) -> Result<ActivationResult, RpcError> {
        self.rpc("install_recipe", &new_recipe)
    }

    /// graphviz description of the dataflow graph
    pub fn graphviz(&mut self) -> String {
        self.rpc("graphviz", &())
    }

    /// Remove a specific node from the graph.
    pub fn remove_node(&mut self, node: NodeIndex) {
        self.rpc("remove_node", &node)
    }

    /*
    /// Get a function that can validate tokens.
    pub fn get_validator(&self) -> Box<Fn(&::dataflow::checktable::Token) -> bool> {
        let descriptor: ControllerDescriptor =
            serde_json::from_slice(&self.authority.get_leader().unwrap().1).unwrap();
        let checktable = checktable::CheckTableClient::connect(
            descriptor.checktable_addr,
            client::Options::default(),
        ).unwrap();
        Box::new(move |t: &checktable::Token| checktable.validate_token(t.clone()).unwrap())
    }
    */

    /// Close all connections opened by this handle.
    ///
    /// Note that this method gets called automatically when the handle is dropped.
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
