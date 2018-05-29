use consensus::Authority;
use dataflow::prelude::*;

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use api::prelude::*;
use controller::Event;
use futures::{self, Future, Sink};
use tokio;

/// A handle to a controller that is running in the same process as this one.
pub struct LocalControllerHandle<A: Authority> {
    c: ControllerHandle<A>,
    event_tx: futures::sync::mpsc::UnboundedSender<Event>,
    runtime: tokio::runtime::Runtime,
}

impl<A: Authority> Deref for LocalControllerHandle<A> {
    type Target = ControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        &self.c
    }
}

impl<A: Authority> DerefMut for LocalControllerHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.c
    }
}

impl<A: Authority> LocalControllerHandle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        event_tx: futures::sync::mpsc::UnboundedSender<Event>,
        rt: tokio::runtime::Runtime,
    ) -> Self {
        LocalControllerHandle {
            c: ControllerHandle::make(authority).unwrap(),
            event_tx,
            runtime: rt,
        }
    }

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

            self.runtime.spawn(
                self.event_tx
                    .clone()
                    .send(Event::ManualMigration(b))
                    .map(|_| ())
                    .map_err(|e| panic!(e)),
            );
            match rx.recv() {
                Ok(ret) => return ret,
                Err(_) => ::std::thread::sleep(::std::time::Duration::from_millis(100)),
            }
        }
    }

    /// Install a new set of policies on the controller.
    pub fn set_security_config(&mut self, p: String) {
        let url = match self.c.url() {
            Some(ref url) => String::from(*url),
            None => panic!("url not defined"),
        };

        self.rpc("set_security_config", &(p, url)).unwrap()
    }

    /// Install a new set of policies on the controller.
    pub fn create_universe(&mut self, context: HashMap<String, DataType>) {
        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        self.rpc::<_, ()>("create_universe", &context).unwrap();

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
        let mut table = self.table(&bname).unwrap();

        table.insert(record).unwrap();
    }

    /// Inform the local instance that it should exit, and wait for that to happen
    pub fn shutdown_and_wait(mut self) {
        self.c.shutdown();
        self.event_tx.send(Event::Shutdown).wait().unwrap();
        self.runtime.shutdown_on_idle().wait().unwrap();
    }

    /// Wait for associated local instance to exit (presumably with an error).
    pub fn wait(self) {
        self.runtime.shutdown_on_idle().wait().unwrap();
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

        let mut c = ControllerBuilder::default().build_local().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
        for _ in 0..250 {
            let _ = c.table("a").unwrap();
        }
    }
}
