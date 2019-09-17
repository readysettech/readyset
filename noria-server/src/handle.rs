use crate::controller::migrate::Migration;
use crate::startup::Event;
use dataflow::prelude::*;
use futures_util::future::poll_fn;
use futures_util::sink::SinkExt;
use noria::consensus::Authority;
use noria::prelude::*;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio_io_pool;

/// A handle to a controller that is running in the same process as this one.
pub struct Handle<A: Authority + 'static> {
    c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<tokio::sync::mpsc::UnboundedSender<Event>>,
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
    pub(super) async fn new(
        authority: Arc<A>,
        event_tx: tokio::sync::mpsc::UnboundedSender<Event>,
        kill: Trigger,
        io: tokio_io_pool::Runtime,
    ) -> Result<Self, failure::Error> {
        let c = ControllerHandle::make(authority).await?;
        Ok(Handle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            iopool: Some(io),
        })
    }

    /// A future that resolves when the controller can accept more messages.
    ///
    /// When this future resolves, you it is safe to call any methods on the wrapped
    /// `ControllerHandle` that require `poll_ready` to have returned `Async::Ready`.
    pub async fn ready(&mut self) -> Result<(), failure::Error> {
        poll_fn(|cx| self.poll_ready(cx)).await
    }

    #[cfg(test)]
    pub(super) async fn backend_ready<E>(&mut self) -> Result<(), E> {
        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.event_tx.send(Event::IsReady(tx)).await.unwrap();
            if rx.await.unwrap() {
                break;
            }

            tokio::timer::delay(time::Instant::now() + time::Duration::from_millis(50)).await;
        }
    }

    #[doc(hidden)]
    pub async fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (ret_tx, ret_rx) = tokio::sync::oneshot::channel();
        let (fin_tx, fin_rx) = tokio::sync::oneshot::channel();
        let b = Box::new(move |m: &mut Migration| {
            if ret_tx.send(f(m)).is_err() {
                unreachable!("could not return migration result");
            }
        });

        self.event_tx
            .as_mut()
            .unwrap()
            .send(Event::ManualMigration { f: b, done: fin_tx })
            .await
            .unwrap();

        fin_rx.await.unwrap();
        ret_rx.await.unwrap()
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub async fn set_security_config(&mut self, p: String) -> Result<(), failure::Error> {
        self.rpc("set_security_config", p, "failed to set security config")
            .await
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub async fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> Result<(), failure::Error> {
        let mut c = self.c.clone().unwrap();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        let _ = self
            .rpc::<_, ()>(
                "create_universe",
                &context,
                "failed to create security universe",
            )
            .await?;

        // Write to Context table
        let bname = match context.get("group") {
            None => format!("UserContext_{}", uid.to_string()),
            Some(g) => format!("GroupContext_{}_{}", g.to_string(), uid.to_string()),
        };

        let mut fields: Vec<_> = context.keys().collect();
        fields.sort();
        let record: Vec<DataType> = fields.iter().map(|&f| context[f].clone()).collect();

        let table = c.table(&bname).await?;
        table
            .insert(record)
            .await
            .map_err(|e| format_err!("failed to make table: {:?}", e))
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

impl<A: Authority> Drop for Handle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    #[cfg_attr(not(debug_assertions), allow_fail)]
    fn limit_mutator_creation() {
        use crate::Builder;
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = Builder::default().start_simple().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
        for _ in 0..2500 {
            let _ = c.table("a").unwrap();
        }
    }
}
