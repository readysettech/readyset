use consensus::Authority;
#[cfg(test)]
use controller::migrate::Migration;
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
    event_tx: Option<futures::sync::mpsc::UnboundedSender<Event>>,
    #[allow(dead_code)]
    runtime: tokio::runtime::Runtime,
    shutdown_rx: Option<futures::sync::oneshot::Receiver<()>>,
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
        shutdown_rx: futures::sync::oneshot::Receiver<()>,
    ) -> Self {
        LocalControllerHandle {
            c: ControllerHandle::make(authority).unwrap(),
            event_tx: Some(event_tx),
            runtime: rt,
            shutdown_rx: Some(shutdown_rx),
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_until_ready(&mut self) {
        let mut snd = self.event_tx.clone().unwrap();
        loop {
            let (tx, rx) = futures::sync::oneshot::channel();
            snd = snd.send(Event::IsReady(tx)).wait().unwrap();
            match rx.wait() {
                Ok(true) => break,
                Ok(false) => {
                    use std::{thread, time};
                    thread::sleep(time::Duration::from_millis(50));
                    continue;
                }
                Err(e) => unreachable!("{:?}", e),
            }
        }
    }

    #[cfg(test)]
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (ret_tx, ret_rx) = futures::sync::oneshot::channel();
        let (fin_tx, fin_rx) = futures::sync::oneshot::channel();
        let b = Box::new(move |m: &mut Migration| -> () {
            if ret_tx.send(f(m)).is_err() {
                unreachable!("could not return migration result");
            }
        });

        self.event_tx
            .clone()
            .unwrap()
            .send(Event::ManualMigration { f: b, done: fin_tx })
            .map(|_| ())
            .wait()
            .unwrap();

        match fin_rx.wait() {
            Ok(()) => ret_rx.wait().unwrap(),
            Err(e) => unreachable!("{:?}", e),
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
    pub fn shutdown_and_wait(&mut self) {
        if let Some(event_tx) = self.event_tx.take() {
            self.c.shutdown();
            event_tx.send(Event::Shutdown).wait().unwrap();
            if let Some(shutdown_rx) = self.shutdown_rx.take() {
                // if the user has already called .wait()
                shutdown_rx.wait().unwrap();
            }
            //self.runtime.shutdown_now().wait().unwrap();
        }
    }

    /// Wait for associated local instance to exit (presumably with an error).
    pub fn wait(mut self) {
        self.shutdown_rx.take().unwrap().wait().unwrap();
        //self.runtime.shutdown_on_idle().wait().unwrap();
    }
}

impl<A: Authority> Drop for LocalControllerHandle<A> {
    fn drop(&mut self) {
        self.shutdown_and_wait();
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
