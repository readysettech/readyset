#[cfg(test)]
use crate::controller::migrate::Migration;
use crate::controller::Event;
use dataflow::prelude::*;
use futures::{self, Future};
use noria::consensus::Authority;
use noria::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio_io_pool;

/// A handle to a local controller that also wraps the tokio runtime.
pub struct LocalSyncControllerHandle<A: Authority + 'static> {
    pub(crate) lch: LocalControllerHandle<A>,
    pub(crate) rt: Option<tokio::runtime::Runtime>,
}

impl<A: Authority> Deref for LocalSyncControllerHandle<A> {
    type Target = LocalControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        &self.lch
    }
}

impl<A: Authority> DerefMut for LocalSyncControllerHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lch
    }
}

impl<A: Authority> Drop for LocalSyncControllerHandle<A> {
    fn drop(&mut self) {
        self.lch.shutdown();
        self.rt.take().unwrap().shutdown_on_idle().wait().unwrap();
    }
}

/// A handle to a controller that is running in the same process as this one.
pub struct LocalControllerHandle<A: Authority + 'static> {
    c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<futures::sync::mpsc::UnboundedSender<Event>>,
    kill: Option<Trigger>,
    iopool: Option<tokio_io_pool::Runtime>,
    executor: tokio::runtime::TaskExecutor,
}

impl<A: Authority> Deref for LocalControllerHandle<A> {
    type Target = ControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        self.c.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for LocalControllerHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.c.as_mut().unwrap()
    }
}

impl<A: Authority> LocalControllerHandle<A> {
    /// Run the given `Future` on the runtime that is running this worker.
    ///
    /// This is helpful if you want to run futures returned by `ControllerHandle`, since those
    /// cannot directly be waited on.
    pub fn run<F>(&mut self, fut: F) -> Result<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send,
        F::Error: Send,
    {
        let (tx, rx) = futures::sync::oneshot::channel();
        self.executor
            .spawn(fut.then(move |r| tx.send(r)).map_err(|_| unreachable!()));
        rx.wait().expect("controller handle went away")
    }

    /// Bundle up the given runtime with this controller handle.
    pub fn to_sync(self, rt: tokio::runtime::Runtime) -> LocalSyncControllerHandle<A> {
        LocalSyncControllerHandle {
            lch: self,
            rt: Some(rt),
        }
    }

    /// Enumerate all known base tables.
    ///
    /// See [`noria::ControllerHandle::inputs`].
    pub fn inputs(&mut self) -> Result<BTreeMap<String, NodeIndex>, failure::Error> {
        let fut = self.c.as_mut().unwrap().inputs();
        self.run(fut)
    }

    /// Enumerate all known external views.
    ///
    /// See [`noria::ControllerHandle::outputs`].
    pub fn outputs(&mut self) -> Result<BTreeMap<String, NodeIndex>, failure::Error> {
        let fut = self.c.as_mut().unwrap().outputs();
        self.run(fut)
    }

    /// Get a handle to a [`noria::Table`].
    ///
    /// See [`noria::ControllerHandle::table`].
    pub fn table<S: AsRef<str>>(&mut self, table: S) -> Result<noria::Table, failure::Error> {
        let fut = self.c.as_mut().unwrap().table(table.as_ref());
        self.run(fut)
    }

    /// Get a handle to a [`noria::View`].
    ///
    /// See [`noria::ControllerHandle::view`].
    pub fn view<S: AsRef<str>>(&mut self, view: S) -> Result<noria::View, failure::Error> {
        let fut = self.c.as_mut().unwrap().view(view.as_ref());
        self.run(fut)
    }

    /// Install a Noria recipe.
    ///
    /// See [`noria::ControllerHandle::install_recipe`].
    pub fn install_recipe<S: AsRef<str>>(
        &mut self,
        r: S,
    ) -> Result<noria::ActivationResult, failure::Error> {
        let fut = self.c.as_mut().unwrap().install_recipe(r.as_ref());
        self.run(fut)
    }

    /// Extend the Noria recipe.
    ///
    /// See [`noria::ControllerHandle::extend_recipe`].
    pub fn extend_recipe<S: AsRef<str>>(
        &mut self,
        r: S,
    ) -> Result<noria::ActivationResult, failure::Error> {
        let fut = self.c.as_mut().unwrap().extend_recipe(r.as_ref());
        self.run(fut)
    }

    /// Fetch a graphviz description of the dataflow graph.
    ///
    /// See [`noria::ControllerHandle::graphviz`].
    pub fn graphviz(&mut self) -> Result<String, failure::Error> {
        let fut = self.c.as_mut().unwrap().graphviz();
        self.run(fut)
    }
}

impl<A: Authority + 'static> LocalControllerHandle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        event_tx: futures::sync::mpsc::UnboundedSender<Event>,
        kill: Trigger,
        io: tokio_io_pool::Runtime,
    ) -> impl Future<Item = Self, Error = failure::Error> {
        ControllerHandle::make(authority).map(move |c| LocalControllerHandle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            iopool: Some(io),
            executor: unimplemented!(),
        })
    }

    #[cfg(test)]
    pub(crate) fn ready(self) -> impl Future<Item = Self, Error = ()> {
        let snd = self.event_tx.clone().unwrap();
        future::loop_fn((self, snd), |(this, mut snd)| {
            let (tx, rx) = futures::sync::oneshot::channel();
            snd.unbounded_send(Event::IsReady(tx)).unwrap();
            rx.and_then(|v| {
                if v {
                    future::Loop::Break(this)
                } else {
                    use std::time;
                    tokio::timer::Delay::new(time::Instant::now() + time::Duration::from_millis(50))
                        .map(move |_| Loop::Continue((this, snd)))
                }
            })
        })
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
            .unbounded_send(Event::ManualMigration { f: b, done: fin_tx })
            .unwrap();

        match fin_rx.wait() {
            Ok(()) => ret_rx.wait().unwrap(),
            Err(e) => unreachable!("{:?}", e),
        }
    }

    /// Install a new set of policies on the controller.
    pub fn set_security_config(
        &mut self,
        p: String,
    ) -> impl Future<Item = (), Error = failure::Error> {
        self.rpc("set_security_config", p, "failed to set security config")
    }

    /// Install a new set of policies on the controller.
    pub fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> impl Future<Item = (), Error = failure::Error> {
        let mut c = self.c.clone().unwrap();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        self.rpc::<_, ()>(
            "create_universe",
            &context,
            "failed to create security universe",
        )
        .and_then(move |_| {
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

            c.table(&bname).and_then(|mut table| {
                table
                    .insert(record)
                    .map_err(|e| format_err!("failed to make table: {:?}", e))
            })
        })
    }

    /// Inform the local instance that it should exit.
    pub fn shutdown(&mut self) {
        if let Some(io) = self.iopool.take() {
            drop(self.c.take());
            drop(self.event_tx.take());
            drop(self.kill.take());
            io.shutdown_on_idle();
        }
    }
}

impl<A: Authority> Drop for LocalControllerHandle<A> {
    fn drop(&mut self) {
        drop(self.event_tx.take());
        if let Some(io) = self.iopool.take() {
            io.shutdown_on_idle();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    #[cfg_attr(not(debug_assertions), allow_fail)]
    fn limit_mutator_creation() {
        use crate::controller::ControllerBuilder;
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = ControllerBuilder::default().build_local().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
        for _ in 0..2500 {
            let _ = c.table("a").unwrap();
        }
    }
}
