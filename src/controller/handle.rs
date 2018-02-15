use consensus::{Authority, LocalAuthority};
use dataflow::checktable;
use dataflow::prelude::*;
use dataflow::statistics::GraphStats;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures::Stream;
use hyper::{self, Client};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use tarpc::sync::client::{self, ClientExt};
use tokio_core::reactor::Core;

use controller::{ControlEvent, ControllerDescriptor, WorkerEvent};
use controller::inner::RpcError;
use controller::getter::{RemoteGetter, RemoteGetterBuilder};
use controller::mutator::{Mutator, MutatorBuilder};
use controller::recipe::ActivationResult;

/// `ControllerHandle` is a handle to a Controller.
pub struct ControllerHandle<A: Authority> {
    pub(super) url: Option<String>,
    pub(super) authority: Arc<A>,
    pub(super) local_controller: Option<(Sender<ControlEvent>, JoinHandle<()>)>,
    pub(super) local_worker: Option<(Sender<WorkerEvent>, JoinHandle<()>)>,
}
impl<A: Authority> ControllerHandle<A> {
    /// Creates a `ControllerHandle` that bootstraps a connection to Soup via the configuration
    /// stored in the `Authority` passed as an argument.
    pub fn new(authority: A) -> Self {
        ControllerHandle {
            url: None,
            authority: Arc::new(authority),
            local_controller: None,
            local_worker: None,
        }
    }

    fn rpc<Q: Serialize, R: DeserializeOwned>(&mut self, path: &str, request: &Q) -> R {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        loop {
            if self.url.is_none() {
                let descriptor: ControllerDescriptor =
                    serde_json::from_slice(&self.authority.get_leader().unwrap().1).unwrap();
                self.url = Some(format!("http://{}", descriptor.external_addr));
            }
            let url = format!("{}/{}", self.url.as_ref().unwrap(), path);

            let mut r = hyper::Request::new(hyper::Method::Post, url.parse().unwrap());
            r.set_body(serde_json::to_vec(request).unwrap());
            let res = core.run(client.request(r)).unwrap();
            if res.status() == hyper::StatusCode::ServiceUnavailable {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            if res.status() != hyper::StatusCode::Ok {
                self.url = None;
                continue;
            }

            let body = core.run(res.body().concat2()).unwrap();
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
    pub fn get_getter_builder(&mut self, node: NodeIndex) -> Option<RemoteGetterBuilder> {
        let rgb: Option<RemoteGetterBuilder> = self.rpc("getter_builder", &node);
        rgb.map(|mut rgb| {
            for &mut (_, ref mut is_local) in &mut rgb.shards {
                *is_local &= self.local_controller.is_some();
            }
            rgb
        })
    }

    /// Obtain a `RemoteGetter`.
    pub fn get_getter(&mut self, node: NodeIndex) -> Option<RemoteGetter> {
        self.get_getter_builder(node).map(|g| g.build())
    }

    /// Obtain a MutatorBuild that can be used to construct a Mutator to perform writes and deletes
    /// from the given base node.
    pub fn get_mutator_builder(&mut self, base: NodeIndex) -> Result<MutatorBuilder, Box<Error>> {
        Ok(self.rpc("mutator_builder", &base))
    }

    /// Obtain a Mutator
    pub fn get_mutator(&mut self, base: NodeIndex) -> Result<Mutator, Box<Error>> {
        self.get_mutator_builder(base)
            .map(|m| m.build("0.0.0.0:0".parse().unwrap()))
    }

    /// Initiaties log recovery by sending a
    /// StartRecovery packet to each base node domain.
    pub fn recover(&mut self) {
        self.rpc("recover", &())
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
}
impl<A: Authority> Drop for ControllerHandle<A> {
    fn drop(&mut self) {
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
