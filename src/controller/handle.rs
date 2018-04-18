use consensus::{Authority, LocalAuthority};
use dataflow::checktable;
use dataflow::prelude::*;
use dataflow::statistics::GraphStats;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use assert_infrequent;
use futures::Stream;
use hyper::{self, Client};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use tarpc::sync::client::{self, ClientExt};
use tokio_core::reactor::Core;

use controller::getter::{GetterRpc, RemoteGetter, RemoteGetterBuilder};
use controller::inner::RpcError;
use controller::mutator::{Mutator, MutatorBuilder, MutatorRpc};
use controller::recipe::ActivationResult;
use controller::{ControlEvent, ControllerDescriptor, WorkerEvent};

/// `ControllerHandle` is a handle to a Controller.
pub struct ControllerHandle<A: Authority> {
    url: Option<String>,
    local_port: Option<u16>,
    authority: Arc<A>,
    pub(super) local_controller: Option<(Sender<ControlEvent>, JoinHandle<()>)>,
    pub(super) local_worker: Option<(Sender<WorkerEvent>, JoinHandle<()>)>,
    getters: HashMap<SocketAddr, GetterRpc>,
    domains: HashMap<Vec<SocketAddr>, MutatorRpc>,
    reactor: Core,
    client: Client<hyper::client::HttpConnector>,
}

impl<A: Authority> ControllerHandle<A> {
    pub(super) fn make(authority: Arc<A>) -> Self {
        let core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        ControllerHandle {
            url: None,
            local_port: None,
            authority: authority,
            local_controller: None,
            local_worker: None,
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

    fn rpc<Q: Serialize, R: DeserializeOwned>(&mut self, path: &str, request: &Q) -> R {
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

        let rgb: Option<RemoteGetterBuilder> = self.rpc("getter_builder", &name);
        rgb.map(|mut rgb| {
            for &mut (_, ref mut is_local) in &mut rgb.shards {
                *is_local &= self.local_controller.is_some();
            }
            rgb
        })
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
    pub fn get_statistics(&mut self) -> GraphStats {
        self.rpc("get_statistics", &())
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

    /// Wait for associated local instance to exit (presumably forever).
    pub fn wait(mut self) {
        self.local_controller.take().unwrap().1.join().unwrap();
        self.local_worker.take().unwrap().1.join().unwrap();
    }

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
}
impl ControllerHandle<LocalAuthority> {
    #[cfg(test)]
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: for<'a> FnMut(&'a mut ::controller::migrate::Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        use controller::migrate::Migration;
        use std::boxed::FnBox;
        use std::sync::Mutex;

        let f = Arc::new(Mutex::new(Some(f)));
        loop {
            let (tx, rx) = ::std::sync::mpsc::channel();
            let f = f.clone();
            let b = Box::new(move |m: &mut Migration| {
                let mut f = f.lock().unwrap().take().unwrap();
                tx.send(f(m)).unwrap();
            })
                as Box<for<'a, 's> FnBox(&'a mut Migration<'s>) + Send + 'static>;

            self.local_controller
                .as_mut()
                .unwrap()
                .0
                .send(ControlEvent::ManualMigration(b))
                .unwrap();

            match rx.recv() {
                Ok(ret) => return ret,
                Err(_) => ::std::thread::sleep(::std::time::Duration::from_millis(100)),
            }
        }
    }

    /// Install a new set of policies on the controller.
    pub fn set_security_config(&mut self, p: String) {
        let url = match self.url {
            Some(ref url) => url.clone(),
            None => panic!("url not defined"),
        };

        self.rpc("set_security_config", &(p, url))
    }

    /// Install a new set of policies on the controller.
    pub fn create_universe(&mut self, context: HashMap<String, DataType>) {
        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        self.rpc::<_, ()>("create_universe", &context);

        // Write to Context table
        let bname = match context.get("group") {
            None => format!("UserContext_{}", uid.to_string()),
            Some(g) => format!("GroupContext_{}_{}", g.to_string(), uid.to_string()),
        };

        let mut fields: Vec<_> = context.keys().collect();
        fields.sort();
        let record: Vec<DataType> = fields
            .iter()
            .map(|&f| context.get(f).unwrap().clone())
            .collect();
        let mut mutator = self.get_mutator(&bname).unwrap();

        mutator.put(record).unwrap();
    }
}
impl<A: Authority> Drop for ControllerHandle<A> {
    fn drop(&mut self) {
        self.getters.clear();
        self.domains.clear();
        if let Some((sender, join_handle)) = self.local_controller.take() {
            let _ = sender.send(ControlEvent::Shutdown);
            let _ = join_handle.join();
        }
        if let Some((sender, join_handle)) = self.local_worker.take() {
            let _ = sender.send(WorkerEvent::Shutdown);
            let _ = join_handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    fn limit_mutator_creation() {
        use controller::ControllerBuilder;
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = ControllerBuilder::default().build_local();
        assert!(c.install_recipe(r_txt.to_owned()).is_ok());
        for _ in 0..250 {
            let _ = c.get_mutator_builder("a").unwrap();
        }
    }
}
