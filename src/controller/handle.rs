use consensus::{Authority};
use dataflow::prelude::*;

use std::collections::{HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::{JoinHandle};

use api::prelude::*;
use controller::{ControlEvent, WorkerEvent};

/// A handle to a controller that is running in the same process as this one.
pub struct LocalControllerHandle<A: Authority> {
    c: ControllerHandle<A>,
    controller: Option<(Sender<ControlEvent>, JoinHandle<()>)>,
    worker: Option<(Sender<WorkerEvent>, JoinHandle<()>)>,
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

impl<A: Authority> Drop for LocalControllerHandle<A> {
    fn drop(&mut self) {
        self.c.shutdown();
        if let Some((sender, join_handle)) = self.controller.take() {
            let _ = sender.send(ControlEvent::Shutdown);
            let _ = join_handle.join();
        }
        if let Some((sender, join_handle)) = self.worker.take() {
            let _ = sender.send(WorkerEvent::Shutdown);
            let _ = join_handle.join();
        }
    }
}

impl<A: Authority> LocalControllerHandle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        controller: Option<(Sender<ControlEvent>, JoinHandle<()>)>,
        worker: Option<(Sender<WorkerEvent>, JoinHandle<()>)>,
    ) -> Self {
        LocalControllerHandle {
            c: ControllerHandle::make(authority),
            controller,
            worker,
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

            self.controller
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
        let url = match self.c.url() {
            Some(ref url) => String::from(*url),
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

    /// Wait for associated local instance to exit (presumably forever).
    pub fn wait(mut self) {
        self.controller.take().unwrap().1.join().unwrap();
        self.worker.take().unwrap().1.join().unwrap();
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
            let _ = c.get_mutator("a").unwrap();
        }
    }
}
