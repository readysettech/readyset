#[cfg(test)]
use crate::controller::migrate::Migration;
use crate::controller::Event;
use dataflow::prelude::*;
use noria::consensus::Authority;
use noria::prelude::*;
use noria::SyncControllerHandle;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio::prelude::*;
use tokio_io_pool;

/// A handle to a controller that is running in the same process as this one.
pub struct Handle<A: Authority + 'static> {
    c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<futures::sync::mpsc::UnboundedSender<Event>>,
    kill: Option<Trigger>,
    iopool: Option<tokio_io_pool::Runtime>,
}

impl<A: Authority> Deref for Handle<A> {
    type Target = ControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        self.c.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for Handle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.c.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> Handle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        event_tx: futures::sync::mpsc::UnboundedSender<Event>,
        kill: Trigger,
        io: tokio_io_pool::Runtime,
    ) -> impl Future<Item = Self, Error = failure::Error> {
        ControllerHandle::make(authority).map(move |c| Handle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            iopool: Some(io),
        })
    }

    #[cfg(test)]
    fn ready<E>(self) -> impl Future<Item = Self, Error = E> {
        let snd = self.event_tx.clone().unwrap();
        future::loop_fn((self, snd), |(this, snd)| {
            let (tx, rx) = futures::sync::oneshot::channel();
            snd.unbounded_send(Event::IsReady(tx)).unwrap();
            rx.map_err(|_| unimplemented!("worker loop went away"))
                .and_then(|v| {
                    if v {
                        future::Either::A(future::ok(future::Loop::Break(this)))
                    } else {
                        use std::time;
                        future::Either::B(
                            tokio::timer::Delay::new(
                                time::Instant::now() + time::Duration::from_millis(50),
                            )
                            .map(move |_| future::Loop::Continue((this, snd)))
                            .map_err(|_| unimplemented!("no timer available")),
                        )
                    }
                })
        })
    }

    #[cfg(test)]
    fn migrate<F, T>(&mut self, f: F) -> T
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
    #[must_use]
    fn set_security_config(&mut self, p: String) -> impl Future<Item = (), Error = failure::Error> {
        self.rpc("set_security_config", p, "failed to set security config")
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    fn create_universe(
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

            c.table(&bname).and_then(|table| {
                table
                    .insert(record)
                    .map_err(|e| format_err!("failed to make table: {:?}", e))
                    .map(|_| ())
            })
        })
    }

    /// Inform the local instance that it should exit.
    fn shutdown(&mut self) {
        if let Some(io) = self.iopool.take() {
            drop(self.c.take());
            drop(self.event_tx.take());
            drop(self.kill.take());
            io.shutdown_on_idle();
        }
    }
}

impl<A: Authority> Drop for Handle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A synchronous handle to a worker.
pub struct SyncHandle<A: Authority + 'static> {
    rt: Option<tokio::runtime::Runtime>,
    wh: Handle<A>,
    // this is an Option so we can drop it
    sh: Option<SyncControllerHandle<A, tokio::runtime::TaskExecutor>>,
}

impl<A: Authority> SyncHandle<A> {
    /// Construct a new synchronous handle on top of an existing runtime.
    ///
    /// Note that the given `Handle` must have been created through the given `Runtime`.
    fn from_existing(rt: tokio::runtime::Runtime, wh: Handle<A>) -> Self {
        let sch = wh.sync_handle(rt.executor());
        SyncHandle {
            rt: Some(rt),
            wh,
            sh: Some(sch),
        }
    }

    /// Construct a new synchronous handle on top of an existing external runtime.
    ///
    /// Note that the given `Handle` must have been created through the `Runtime` backing the
    /// given executor.
    fn from_executor(ex: tokio::runtime::TaskExecutor, wh: Handle<A>) -> Self {
        let sch = wh.sync_handle(ex);
        SyncHandle {
            rt: None,
            wh,
            sh: Some(sch),
        }
    }

    /// Stash away the given runtime inside this worker handle.
    fn wrap_rt(&mut self, rt: tokio::runtime::Runtime) {
        self.rt = Some(rt);
    }

    /// Run an operation on the underlying asynchronous worker handle.
    fn on_worker<F, FF>(&mut self, f: F) -> Result<FF::Item, FF::Error>
    where
        F: FnOnce(&mut Handle<A>) -> FF,
        FF: IntoFuture,
        FF::Future: Send + 'static,
        FF::Item: Send + 'static,
        FF::Error: Send + 'static,
    {
        let fut = f(&mut self.wh);
        self.sh.as_mut().unwrap().run(fut)
    }

    #[cfg(test)]
    fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.on_worker(move |w| -> Result<_, ()> { Ok(w.migrate(f)) })
            .unwrap()
    }
}

impl<A: Authority> Deref for SyncHandle<A> {
    type Target = SyncControllerHandle<A, tokio::runtime::TaskExecutor>;
    fn deref(&self) -> &Self::Target {
        self.sh.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for SyncHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.sh.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> Drop for SyncHandle<A> {
    fn drop(&mut self) {
        drop(self.sh.take());
        self.wh.shutdown();
        if let Some(rt) = self.rt.take() {
            rt.shutdown_on_idle().wait().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    #[cfg_attr(not(debug_assertions), allow_fail)]
    fn limit_mutator_creation() {
        use crate::controller::Builder;
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = Builder::default().start_simple().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
        for _ in 0..2500 {
            let _ = c.table("a").unwrap();
        }
    }
}
